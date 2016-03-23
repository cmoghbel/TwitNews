package crawler;

import dao.TrendDAO;
import dao.TweetDAO;
import dao.UserDAO;
import database.MySQL_UI;
import database.SqlConstants;
import datastructures.InvertedIndex;
import domain.*;
import fileIO.FileConstants;
import fileIO.FileIO;
import ranker.NewsRanker;
import twitter4j.*;
import util.TextUtils;

import java.io.IOException;
import java.util.*;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import static util.TwitNewsConstants.ONE_HOUR_IN_MILLIS;
import static util.TwitNewsConstants.ONE_MINUTE_IN_MILLIS;

/**
 * A class for crawling news related trends by starting from the @breakingnews account. Attempts to reuse framework
 * wherever possible. Also computes the tweets rank as it goes along.
 *
 * @author Chris Moghbel (cmoghbel@cs.ucla.edu)
 */
public class NewsCrawler {

// Twitter instances that should be shared throughout the crawler
private static final Twitter twitter = TwitterFactory.getSingleton();
private static final TwitterStream twitterStream = TwitterStreamFactory.getSingleton();

// Objects that should be shared throughout the crawler
private static MySQL_UI sql;
private static final Logger log = Logger.getLogger(StreamCrawler.class.getName());

private static final InvertedIndex<String, Integer> invertedIndex = new InvertedIndex<String, Integer>();

private static final Set<String> stopWords = new HashSet<String>();

// A mapping of trendNames to their id's in the db, global to facilitate access within the StatusListener
//private static final Map<String, Integer> trendNameToId = new HashMap<String, Integer>();

  /**
   * Entry point for the crawler. Sets up necessary data structures and objects and kicks off a crawl.
   *
   * @param args Command line args, none needed.
   */
  public static void main(String[] args) {

    setupLogging();

    loadStopWords();

    sql = createDbConnection(args);

    // This is where the crawling occurs. Happens on a separate thread from the main thread.
    StatusListener statusListener = new StatusListener() {

      private TweetDAO tweetDAO = new TweetDAO(sql);
      private UserDAO userDAO = new UserDAO(sql);
      int tweetsCrawled = 0;
      private Map<Integer, List<TwitNewsTweet>> tweetsByTrendId = new HashMap<Integer, List<TwitNewsTweet>>();
      private Set<TwitNewsUser> users = new HashSet<TwitNewsUser>();

      @Override
      public void onStatus(Status status) {

        int[] trendIdAndScore = getCorrespondingTrendId(status);
        int correspondingTrendId = trendIdAndScore[0];
        int tfidf = trendIdAndScore[1];

        if (correspondingTrendId == 0) {
          return;
        }

        TwitNewsTweet twitNewsTweet = TwitNewsTweetFactory.fromStatus(status, correspondingTrendId);
        twitNewsTweet.setTfidf(tfidf);

        int rank = NewsRanker.computeRank(twitNewsTweet);
        twitNewsTweet.setRank(rank);

        TwitNewsUser twitNewsUser = TwitNewsUserFactory.fromStatus(status);

        // Update the trend id to tweet mapping
        if (tweetsByTrendId.containsKey(correspondingTrendId)) {
          List<TwitNewsTweet> tweetsForTrend = tweetsByTrendId.get(correspondingTrendId);
          tweetsForTrend.add(twitNewsTweet);
          if (status.isRetweet()) {
            tweetsForTrend.add(TwitNewsTweetFactory.fromStatus(status.getRetweetedStatus(), correspondingTrendId));
          }
        }
        else {
          List<TwitNewsTweet> tweetsForTrend = new ArrayList<TwitNewsTweet>();
          tweetsForTrend.add(twitNewsTweet);
          if (status.isRetweet()) {
            tweetsForTrend.add(TwitNewsTweetFactory.fromStatus(status.getRetweetedStatus(), correspondingTrendId));
          }
          tweetsByTrendId.put(correspondingTrendId, tweetsForTrend);
        }
        ++tweetsCrawled;

        users.add(twitNewsUser);

        // Let's insert the tweets or users if we get more than 250 of any of them
        log.info(status.getText());
        if (tweetsCrawled >= 250) {
          for (Integer trendId : tweetsByTrendId.keySet()) {
            insertTweets(tweetsByTrendId.get(trendId), trendId);
          }
        }
        if (users.size() >= 250) {
          insertUsers();
        }
      }

      /**
       * Gets the corresponding trend id for a given status.
       *
       * @param status The status to get the trend if for.
       *
       * @return int The corresponding trend id.
       */
      private int[] getCorrespondingTrendId(Status status) {
        return searchInvertedIndex(status.getText());
      }

      private int[] searchInvertedIndex(String text) {
        Map<Integer, Integer> counters = new HashMap<Integer, Integer>();
        Set<String> keywords = TextUtils.parseKeywordsFromTweetText(text, stopWords);
        for (String keyword : keywords) {
          if (invertedIndex.containsKey(keyword)) {
            Set<Integer> matchingTrendIds = invertedIndex.get(keyword);
            for (Integer matchingTrendId : matchingTrendIds) {
              if (counters.containsKey(matchingTrendId)) {
                int count = counters.get(matchingTrendId);
                ++count;
                counters.put(matchingTrendId, count);
              }
              else {
                counters.put(matchingTrendId, 1);
              }
            }
          }
        }
        int highestRank = 0;
        int highestRankedTrendId = 0;
        for (Integer trendId : counters.keySet()) {
          int rank = counters.get(trendId);
          if (rank > highestRank) {
            highestRank = rank;
            highestRankedTrendId = trendId;
          }
        }
        int score = 0;
        if (counters.containsKey(highestRankedTrendId)) {
          score = counters.get(highestRankedTrendId);
        }
        return new int[]{highestRankedTrendId, score};
      }

      /**
       * Inserts tweets into the db.
       *
       * @param tweets The tweet to insert.
       * @param correspondingTrendId The tweet's corresponding trend id.
       */
      private void insertTweets(Collection<TwitNewsTweet> tweets, int correspondingTrendId) {
        try {
          tweetDAO.insertTweets(tweets, correspondingTrendId);
          log.info("Successfully logged 250 tweets.");
          tweets.clear();
          tweetsCrawled = 0;
        }
        catch (Exception e) {
          log.severe(e.toString());
          log.severe("Failure persisting tweets. Will try again on next received status.");
        }
      }

      /**
       * Inserts users into the db.
       */
      private void insertUsers() {
        try {
          userDAO.insertUsers(users);
          log.info("Successfully logged 250 users.");
          users.clear();
        }
        catch (Exception e) {
          log.severe(e.toString());
          log.severe("Failure persisting users. Will try again on next received status.");
        }
      }

      @Override
      public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}

      @Override
      public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}

      @Override
      public void onScrubGeo(long userId, long upToStatusId) {}

      @Override
      public void onException(Exception ex) {
        ex.printStackTrace();
      }
    };
    twitterStream.addListener(statusListener);

    try {
      //noinspection InfiniteLoopStatement
      while(true) {
        log.info("Crawling trends for the US via Stream API...");

        refreshStreamApiTrackKeywords();

        waitOneHour();

        twitterStream.cleanUp();
//        trendNameToId.clear();
      }
    }
    finally {
      twitterStream.shutdown();
      log.info("Finished crawling current trends!");
    }
  }

  /**
   * Queries twitter for the latest trends and sets the track keywords to the trend names.
   *
   * @see "http://developer.yahoo.com/geo/geoplanet/guide/concepts.html"
   */
  private static void refreshStreamApiTrackKeywords() {

    invertedIndex.clear();

    Set<String> trends = getTrendsFromRestAPI();

    Set<String> keywords = new LinkedHashSet<String>();
    for (String trendText : trends) {
      TwitNewsTrend trend = new TwitNewsTrend();
      trend.setName(trendText);
      int trendId = attemptToInsertTrend(trend);
      Set<String> keywordsForTrend = TextUtils.parseKeywordsFromTweetText(trendText, stopWords);
      keywords.addAll(keywordsForTrend);
      invertedIndex.putAll(keywordsForTrend, trendId);
    }

    // Insert trends and create keyword array for twitter stream filtering.
    String[] trendStrings = new String[keywords.size()];
    int i = 0;
    for (String  keyword : keywords) {
      trendStrings[i] = keyword;
      ++i;
    }

    // Let's just grab tweets from the stream matching the trend keywords!
    FilterQuery filter = new FilterQuery();
    filter.track(trendStrings);
    twitterStream.filter(filter);
  }

  /**
   * Gets the current trends via the REST API.
   *
   * @return A set of the latest tweets from @breakingnews.
   */
  private static Set<String> getTrendsFromRestAPI() {
    Set<String> trends = new LinkedHashSet<String>();
    try {
      ResponseList<Status> statuses = twitter.getUserTimeline("breakingnews");
      for (Status status : statuses) {
        String tweet = status.getText().trim();
        trends.add(tweet);
      }
    }
    catch (TwitterException e) {
      e.printStackTrace();
    }
    return trends;
  }

  /**
   * Attempts to insert a trend until it succeeds.
   *
   * @param twitNewsTrend The trend to insert.
   *
   * @return int the trend if of the just inserted trend in the db.
   */
  private static int attemptToInsertTrend(TwitNewsTrend twitNewsTrend) {
    TrendDAO trendDAO = new TrendDAO(sql);
    int trendId = 0;
    boolean successfullyInsertedTrend = false;
    while (!successfullyInsertedTrend) {
      try {
        trendId = trendDAO.insertTrend(twitNewsTrend);
        successfullyInsertedTrend = true;
      }
      catch (Exception e) {
        log.severe("There was a problem persisting the trend. Lets try again!");
        e.printStackTrace();
      }
    }
    return trendId;
  }

  /**
   * Sets up logging for the crawler. INFO and above will be logged to out file as well as the console, while
   * SEVERE and up will be logged to err file.
   */
  private static void setupLogging() {
    try {
      FileHandler out = new FileHandler("twitnews_out_stream" + System.currentTimeMillis());
      FileHandler err = new FileHandler("twitnews_err_stream" + System.currentTimeMillis());

      out.setLevel(Level.INFO);
      err.setLevel(Level.WARNING);

      log.addHandler(out);
      log.addHandler(err);
    }
    catch (IOException e) {
      e.printStackTrace();
      System.err.println("Error setting up logging! Can't recover, exiting!");
      System.exit(1);
    }
  }

  private static void loadStopWords() {
    FileIO fileIO = new FileIO(FileConstants.STOP_WORDS);
    fileIO.getWordSet(stopWords);
  }

  /**
   * Creates a new connection to the db.
   *
   * @param args A copy of the command line args.
   * @return The connection to the db.
   */
  private static MySQL_UI createDbConnection(String[] args) {
    String host;
    String port;
    String user;
    String password;

    if (args.length < 3) {
      log.info("No host parameter provided, using default.");
      host = SqlConstants.HOST;
    }
    else {
      host = args[2];
    }

    if (args.length < 4) {
      log.info("No port parameter provided, using default.");
      port = SqlConstants.PORT;
    }
    else {
      port = args[3];
    }

    if (args.length < 5) {
      log.info("No user parameter provided, using default.");
      user = SqlConstants.USER;
    }
    else {
      user = args[4];
    }

    if (args.length < 6) {
      log .info("No sql password provided, using default");
      password = SqlConstants.PASSWORD;
    }
    else {
      password = args[5];
    }

    return new MySQL_UI(host, port, user, password);
  }

  /**
   * Blocks for 1 minute.
   */
  private static void waitOneMinute() {
    try {
      Thread.sleep(ONE_MINUTE_IN_MILLIS);
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Blocks for 1 hour.
   */
  private static void waitOneHour() {
    int timeWaitedInMillis = 0;
    while (timeWaitedInMillis < ONE_HOUR_IN_MILLIS) {
      waitOneMinute();
      timeWaitedInMillis += ONE_MINUTE_IN_MILLIS;
    }
  }

}
