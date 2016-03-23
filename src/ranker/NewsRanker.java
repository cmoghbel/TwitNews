package ranker;

import dao.TrendDAO;
import dao.TweetDAO;
import database.MySQL_UI;
import database.SqlConstants;
import domain.TwitNewsTrend;
import domain.TwitNewsTweet;

import java.util.List;

import static util.TwitNewsConstants.MAX_RETWEENT_COUNT;
import static util.TwitNewsConstants.MAX_TWITTER_FOLLOWERS;

/**
 * Created by IntelliJ IDEA.
 * User: Shahin
 * Date: 11/8/11
 * Time: 4:44 PM
 * To change this template use File | Settings | File Templates.
 */
public class NewsRanker {

  /**
   * Entry point for the ranker.
   *
   * @param args Command line arguments.
   */
  public static void main(String[] args) {

    // Create appropriate DAO objects
    MySQL_UI sql = new MySQL_UI(SqlConstants.HOST, SqlConstants.PORT, SqlConstants.USER, SqlConstants.PASSWORD);
    TrendDAO trendDAO = new TrendDAO(sql);
    TweetDAO tweetDAO = new TweetDAO(sql);

    // Fetch the trends
    List<TwitNewsTrend> trends = trendDAO.fetchAllTrends();

    for (TwitNewsTrend trend : trends) {
      System.out.println("Ranking tweets for trend: " + trend.getName());
      List<TwitNewsTweet> tweets = tweetDAO.fetchTweets(trend.getTrendId());

      for (TwitNewsTweet tweet : tweets) {

        // Update the new rank
        int newRankScore = computeRank(tweet);
        tweet.setRank(newRankScore);

      }
      tweetDAO.updateRanks(tweets);
      tweets.clear();
    }
    sql.closeConnection();
  }

  public static int computeRank(TwitNewsTweet tweet) {
     // Weights for each of the various parameters we are considering
    // These should add up to 1
    double linkFactor = 0.05;
    double retweetFactor = 0.15;
    double followerFactor = 0.1;
    double trustFactor = 0.2;
    double keywordMatchFactor = .5;

    double keywordMatchScore = tweet.getTfidf() * keywordMatchFactor;

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

    // Scale by 1 million and cast to int to avoid sql decimal issues
    return (int) (1000000 * (keywordMatchScore + linkScore + trustScore));
  }

}
