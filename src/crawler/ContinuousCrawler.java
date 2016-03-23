package crawler;

import dao.TrendDAO;
import dao.TweetDAO;
import dao.UserDAO;
import database.MySQL_UI;
import database.SqlConstants;
import domain.*;
import twitter4j.*;

import java.io.IOException;
import java.util.*;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import static util.TwitNewsConstants.*;

/**
 * A crawler designed to crawl continuously. Updates the Stream API track keywords every hour with the latest trends
 * for the US.
 *
 * @author Chris Moghbel (cmoghbel@cs.ucla.edu)
 */
public class ContinuousCrawler {

  // Twitter instances that should be shared throughout the crawler
  private static final Twitter twitter = TwitterFactory.getSingleton();
  private static final TwitterStream twitterStream = TwitterStreamFactory.getSingleton();

  // Objects that should be shared throughout the crawler
  private static MySQL_UI sql;
  private static final Logger log = Logger.getLogger(StreamCrawler.class.getName());

  // A mapping of trendNames to their id's in the db, global to facilitate access within the StatusListener
  private static final Map<String, Integer> trendNameToId = new HashMap<String, Integer>();

	/**
	 * Entry point for the crawler. Sets up necessary data structures and objects and kicks off a crawl.
   *
   * @param args Command line args, none needed.
	 */
	public static void main(String[] args) {

    setupLogging();

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

        int correspondingTrendId = getCorrespondingTrendId(status);

        TwitNewsTweet twitNewsTweet = TwitNewsTweetFactory.fromStatus(status, correspondingTrendId);
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
      private int getCorrespondingTrendId(Status status) {
        String text = status.getText().replaceAll(" ", "").trim().toLowerCase();
        for (String key : trendNameToId.keySet()) {
          if (text.contains(key.replace(" ", "").trim().toLowerCase())) {
            return trendNameToId.get(key);
          }
        }
        log.fine("Couldn't find a matching trend.");
        return 0;
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
        trendNameToId.clear();
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

    Trend[] trends = getTrendsFromRestApi();

    // Insert trends and create keyword array for twitter stream filtering.
    String[] trendStrings = new String[trends.length];
    for (int i = 0; i < trends.length; ++i) {
      Trend trend = trends[i];
      int trendId = attemptToInsertTrend(TwitNewsTrendFactory.fromTrend(trend));
      trendNameToId.put(trend.getName().trim().toLowerCase(), trendId);
      trendStrings[i] = trend.getName();
    }

    // Let's just grab tweets from the stream matching the trend keywords!
    FilterQuery filter = new FilterQuery();
    filter.track(trendStrings);
    twitterStream.filter(filter);
  }

  /**
   * Gets the current trends via the REST API.
   *
   * @return An array of trends.
   */
  private static Trend[] getTrendsFromRestApi() {
    Trends trendsForLocation = null;
    boolean successfullyObtainedTrends = false;

    while(!successfullyObtainedTrends) {
      try {
        trendsForLocation = twitter.getLocationTrends(US_WOEID);
        successfullyObtainedTrends = true;
      }
      catch (TwitterException e) {
        log.severe("Error obtaining trends from twitter.");
        e.printStackTrace();
        waitOneMinute();
      }
    }
    return trendsForLocation.getTrends();
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
