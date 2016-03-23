package ranker;

import dao.RankDAO;
import dao.TrendDAO;
import dao.TweetDAO;
import database.MySQL_UI;
import database.SqlConstants;
import domain.TwitNewsRank;
import domain.TwitNewsTrend;
import domain.TwitNewsTweet;
import fileIO.FileConstants;
import fileIO.FileIO;

import java.util.*;

import static util.TwitNewsConstants.MAX_RETWEENT_COUNT;
import static util.TwitNewsConstants.MAX_TWITTER_FOLLOWERS;

/**
 * Computes the rank for tweets and stores them to the database.
 *
 * @author Chris Moghbel (cmoghbel@cs.ucla.edu)
 * @author Lei (Ricky) Jin (rickyjin@cs.ucla.edu)
 */
public class Ranker {

  /**
   * Entry point for the ranker.
   *
   * @param args Command line arguments.
   */
  public static void main(String[] args) {
    MySQL_UI sql = new MySQL_UI(SqlConstants.HOST, SqlConstants.PORT, SqlConstants.USER, SqlConstants.PASSWORD);
    TrendDAO trendDAO = new TrendDAO(sql);
    TweetDAO tweetDAO = new TweetDAO(sql);
    RankDAO rankDAO = new RankDAO(sql);
    List<TwitNewsTrend> trends = trendDAO.fetchAllTrends();
    FileIO badWordsFile1 = new FileIO(FileConstants.PATH_1);
    FileIO badWordsFile2 = new FileIO(FileConstants.PATH_2);

    for (TwitNewsTrend trend : trends) {
      System.out.println("Ranking tweets for trend: " + trend.getName());
      List<TwitNewsTweet> tweets = tweetDAO.fetchTweets(trend.getTrendId());
      Map<String, TwitNewsRank> tweetMap = new HashMap<String, TwitNewsRank>();
      Set<String> badWordSet = new HashSet<String>();
      
      double numTweetsWithLinks = 0.0;
      double numRetweetsForTrend = 0.0;
      
      // collects bad words from the two input text files
      badWordSet = badWordsFile1.getWordSet(badWordSet);
      badWordSet = badWordsFile2.getWordSet(badWordSet);
      
      for (TwitNewsTweet tweet : tweets) {
        int rankScore = computeRank(tweet, trends, badWordSet);
        TwitNewsRank rank = new TwitNewsRank();
        rank.setTrendId(trend.getTrendId());
        rank.setTweetId((int) tweet.getTweetId());
        rank.setRank(rankScore);

        if (tweet.isHasLink() && rankScore > 0) {
          ++numTweetsWithLinks;
        }
        if (tweet.isRetweet() && rankScore > 0) {
          ++numRetweetsForTrend;
        }

        // We want to remove duplicates!
        if (tweetMap.containsKey(tweet.getText())) {
          TwitNewsRank highestRank = tweetMap.get(tweet.getText());
          if (rank.getRank() > highestRank.getRank()) {
            tweetMap.put(tweet.getText(), rank);
          }
        }
        else {
          tweetMap.put(tweet.getText(), rank);
        }
      }
      if (!tweetMap.values().isEmpty()) {
        List<TwitNewsRank> ranks = new ArrayList<TwitNewsRank>();
        for (TwitNewsRank rank : tweetMap.values()) {
          ranks.add(rank);
          if (ranks.size() > 100) {
            rankDAO.insertRanks(ranks);
            ranks.clear();
          }
        }
      }
      computeNewsRank(trend, numTweetsWithLinks, numRetweetsForTrend, tweets.size());
    }
    sql.closeConnection();
  }

  /**
   * The main tweet rank algorithm.
   *
   * @param tweet The tweet to rank.
   * @param trends The trends.
   * @return int The rank of the tweet.
   */
  private static int computeRank(TwitNewsTweet tweet, List<TwitNewsTrend> trends, Set<String> badWordSet) {

    // Weights for each of the various parameters we are considering
    // These should add up to 1
    double linkFactor = 0.1;
    double retweetFactor = 0.3;
    double followerFactor = 0.2;
    double trustFactor = 0.4;

    double linkScore = 0;
    // if a tweet has a link and contains the characters (cont), it's most likely a tweet longer type service
    if (tweet.isHasLink() && !tweet.getText().toLowerCase().contains("(cont)")) {
      linkScore = 1 * linkFactor;
    }

    long retweets = tweet.getRetweetCount();
    if (tweet.isRetweet()) {
      retweets = 0;
    }

    // trust score = a * norm(numRetweets) + b * log(numFollowers)/log(max_twitter_followers)) + c * isVerifiedUser ? 1 : 0
    double trustScore = retweetFactor * (retweets / MAX_RETWEENT_COUNT);
    trustScore += followerFactor * (Math.log(tweet.getNumFollowers()) / Math.log(MAX_TWITTER_FOLLOWERS));
    if (tweet.isVerifiedUser()) {
      trustScore += 1 * trustFactor;
    }

    double spamScore = computeSpamScore(tweet, trends, badWordSet);
    // Scale by 1 million and cast to int to avoid sql decimal issues
    return (int) (1000000 * (linkScore + trustScore - spamScore));
  }

  /**
   * Computes a spam score for the tweet.
   *
   * @param tweet The tweet.
   * @param trends The trends.
   * @param badWordSet A set of bad or spam words to penalize against.
   * @return A ranking indicating how spammy this tweet is.
   */
  private static double computeSpamScore(TwitNewsTweet tweet, List<TwitNewsTrend> trends, Set<String> badWordSet) {
    String[] tokens = tweet.getText().trim().split("");
    String[] words = tweet.getText().trim().split(" ");
    int numChars = tweet.getText().length();
    int numUninterestingChars = 0;
    
    for (TwitNewsTrend trend : trends) {
      if (tweet.getTrendId() != trend.getTrendId()) {
        if (tweet.getText().toLowerCase().contains(trend.getName().toLowerCase())) {
          numUninterestingChars += trend.getName().length();
        }
      }
    }
    
    for (String token : tokens) {
      for (TwitNewsTrend trend : trends) {
        if (!token.trim().toLowerCase().equals(trend.getName().trim().toLowerCase())) {
          if (token.startsWith("#")) {
            numUninterestingChars += token.length();
          }
        }
      }
    }
    
    // add bad words to uninteresting count
    for (String word : words) {
      if (badWordSet.contains(word.trim().toLowerCase())) {
        numUninterestingChars += word.length();
      }
    }
    
    return numUninterestingChars / numChars;
  }

  /**
   * Attempts to rank how news worthy a trend is.
   *
   * @param trend The trend.
   * @param numLinks The number of links encountered when crawling this trend.
   * @param numRetweets The number of retweets encountered when crawling this trend.
   * @param numTweets The number of tweets crawled for this trend.
   */
  private static void computeNewsRank(TwitNewsTrend trend, double numLinks, double numRetweets, int numTweets) {
    double hashtagPenalty = 0;
    if (trend.getName().startsWith("#")) {
      hashtagPenalty =  .05;
    }

    double linksToTweetRatio = numLinks / numTweets;
    double retweetsToTweetsRatio = numRetweets / numTweets;

    if (numTweets != 0) {
      System.out.println("> links to tweet ratio:     " + linksToTweetRatio);
      System.out.println("> retweets to tweets ratio: " + retweetsToTweetsRatio);
      System.out.println("> news rank ratio:          " + (linksToTweetRatio + retweetsToTweetsRatio - hashtagPenalty));
    }
  }

}