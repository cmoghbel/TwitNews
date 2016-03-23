package crawler;

import dao.TrendDAO;
import dao.TweetDAO;
import dao.UserDAO;
import database.MySQL_UI;
import database.SqlConstants;
import domain.*;
import twitter4j.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import static util.TwitNewsConstants.US_WOEID;
import static util.TwitNewsConstants.WORLD_WOEID;

/**
 * A crawler that uses the Twitter Stream API to avoid rate limiting.
 *
 * @see "https://dev.twitter.com/docs/streaming-api"
 *
 * @author Chris Moghbel (cmoghbel@ucla.edu)
 */
public class StreamCrawler {

  // If we are consistently getting tweets at a rate slower than 2 seconds, we should switch trends, otherwise the
  // crawler will take forever to run. This rate will mean each trend should finish in at most about 33 minutes,
  // assuming tweets are coming in at a relatively steady rate.
  private static final int switchRateThreshold = 2000;
  private static final int switchRateThresholdHigh = 15000;

  private static final Twitter twitter = TwitterFactory.getSingleton();
  private static final TwitterStream twitterStream = TwitterStreamFactory.getSingleton();

  private static Set<String> trends = new HashSet<String>();

  private static MySQL_UI sql;

  private static final Logger log = Logger.getLogger(StreamCrawler.class.getName());

  // Necessary variable for switching between trends after a certain amount of tweets have been crawled.
  private static int tweetsToCrawlForTrend = 0;
  private static int tweetsCrawledForTrend = 0;
  private static boolean timeToSwitchTrends = false;

  private static int currentTrendId = 0;

	/**
	 * Entry point for the crawler. Sets up necessary data structures and objects, parses arguments, and kicks off a
   * crawl.
	 *
	 * @param args First argument: true for us only trends, else false. Second argument: number of tweets to crawl per trend.
	 */
	public static void main(String[] args) {

    setupLogging();

    sql = createDbConnection(args);

    // Parse command line args
    boolean getUSTrendsOnly = Boolean.parseBoolean(args[0]);
    tweetsToCrawlForTrend = Integer.parseInt(args[1]);

    // This is where the crawling occurs. Happens on a seperate thread from the main thread.
    StatusListener statusListener = new StatusListener() {

      private long timeOfLastTweet;
      private int numTweetsOverThreshold = 0;
      private int numTweetsOverHighThreshold = 0;
      private TweetDAO tweetDAO = new TweetDAO(sql);
      private UserDAO userDAO = new UserDAO(sql);
      private List<TwitNewsTweet> tweets = new ArrayList<TwitNewsTweet>();
      private Set<TwitNewsTweet> originalTweets = new HashSet<TwitNewsTweet>();
      private Set<TwitNewsUser> users = new HashSet<TwitNewsUser>();

      @Override
      public void onStatus(Status status) {
        TwitNewsTweet twitNewsTweet = TwitNewsTweetFactory.fromStatus(status, currentTrendId);
        TwitNewsUser twitNewsUser = TwitNewsUserFactory.fromStatus(status);
        tweets.add(twitNewsTweet);
        users.add(twitNewsUser);

        // If this is a Retweet, get the original status
        if (status.isRetweet()) {
          Status originalTweet = status.getRetweetedStatus();
          originalTweets.add(TwitNewsTweetFactory.fromStatus(originalTweet, currentTrendId));
        }

        // Let's insert the tweets, users, or the original tweets if we get more than 250 of any of them
        log.info(status.getText());
        if (tweets.size() >= 250 || users.size() >= 250 || originalTweets.size() >= 250) {
          insertTrendsAndUsers();
        }

        ++tweetsCrawledForTrend;
        long millisSinceLastTweet = status.getCreatedAt().getTime() - timeOfLastTweet;
        timeOfLastTweet = status.getCreatedAt().getTime();
        if (millisSinceLastTweet > switchRateThresholdHigh) {
          ++numTweetsOverThreshold;
          ++numTweetsOverHighThreshold;
        }
        else if (millisSinceLastTweet > switchRateThreshold) {
          ++numTweetsOverThreshold;
        }
        else {
          numTweetsOverThreshold = 0;
          numTweetsOverHighThreshold = 0;
        }

        if (tweetsCrawledForTrend >= tweetsToCrawlForTrend) {
          log.info("Got " + tweetsToCrawlForTrend + " tweets for this trend, time to switch to the next one.");
          timeToSwitchTrends = true;
          insertTrendsAndUsers();
        }
        else if (numTweetsOverThreshold > 10) {
          log.info("Tweets coming in too slow, lets switch to the next trend");
          timeToSwitchTrends = true;
          insertTrendsAndUsers();
        }
        else if (numTweetsOverHighThreshold > 1) {
          log.info("Tweets coming in too slow, lets switch to the next trend");
          timeToSwitchTrends = true;
          insertTrendsAndUsers();
        }
      }

      private void insertTrendsAndUsers() {
        // Insert Tweets
        try {
          tweetDAO.insertTweets(tweets, currentTrendId);
          log.info("Successfully logged 250 tweets.");
          tweets.clear();
        }
        catch (Exception e) {
          log.severe(e.toString());
          log.severe("Failure persisting tweets. Will try again.");
        }

        // Insert Users
        try {
          userDAO.insertUsers(users);
          log.info("Successfully logged 250 users.");
          users.clear();
        }
        catch (Exception e) {
          log.severe(e.toString());
          log.severe("Failure persisting users. Will try again.");
        }

        // Insert original tweets
        try {
          tweetDAO.insertTweets(originalTweets, currentTrendId);
          log.info("Successfully inserted 250 original tweets.");
          originalTweets.clear();
        }
        catch (Exception e) {
          log.severe(e.toString());
          log.severe("Failure persisting original tweets. Will try again");
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

    if (getUSTrendsOnly) {
      log.info("Crawling trends for the US via Stream API...");
      crawlTrends(US_WOEID);
    }
    else {
      log.info("Crawling worldwide trends via Stream API...");
      crawlTrends(WORLD_WOEID);
    }
    twitterStream.shutdown();
    log.info("Finished crawling current trends!");
	}

  /**
   * Queries twitter and lists all the current, unique trends for the given location.
   *
   * @param woeid The Yahoo woeid for the location to get trends for.
   *
   * @see "http://developer.yahoo.com/geo/geoplanet/guide/concepts.html"
   */
  private static void crawlTrends(int woeid) {

    // Cache to avoid fetching information about the same trend multiple times
    Set<String> trendNames = new HashSet<String>();

    Trends trendsForLocation = null;
    try {
      trendsForLocation = twitter.getLocationTrends(woeid);
    }
    catch (TwitterException e) {
      log.severe("Error obtaining trends from twitter.");
      e.printStackTrace();
      System.exit(1);
    }

    TrendDAO trendDAO = new TrendDAO(sql);

    for (Trend trend : trendsForLocation.getTrends()) {

      String trendName = trend.getName().trim().toLowerCase();
      TwitNewsTrend twitNewsTrend = new TwitNewsTrend();
      twitNewsTrend.setName(trend.getName());

      if (!trendNames.contains(trendName)) {

        trends.add(trend.getName());
        trendNames.add(trendName);
        attemptToInsertTrend(trendDAO, twitNewsTrend);

        // Let's just grab tweets from the stream matching the trend keyword!
        FilterQuery filter = new FilterQuery();
        filter.track(new String[]{trend.getName()});
        twitterStream.filter(filter);
        while (!timeToSwitchTrends) {
          try {
            Thread.sleep(1000);
          }
          catch (InterruptedException e) {
            log.warning("Interrupted while waiting!");
          }
        }
        twitterStream.cleanUp();
        timeToSwitchTrends = false;
        tweetsCrawledForTrend = 0;
      }
    }
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
   * Attempts to insert a trend up to 3 times before giving up.
   *
   * @param trendDAO TrendDAO object.
   * @param twitNewsTrend The trend to insert.
   */
  private static void attemptToInsertTrend(TrendDAO trendDAO, TwitNewsTrend twitNewsTrend) {
    int tries = 0;
    boolean successfullyInsertedTrend = false;
    while (tries < 3 && ! successfullyInsertedTrend) {
      try {
        ++tries;
        currentTrendId = trendDAO.insertTrend(twitNewsTrend);
        successfullyInsertedTrend = true;
      }
      catch (Exception e) {
        log.severe("There was a problem persisting the trend (try " + tries + "). Lets try again later?");
        e.printStackTrace();
      }
    }
    if (!successfullyInsertedTrend) {
      timeToSwitchTrends = true;
    }
  }

}
