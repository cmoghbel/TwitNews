package crawler;

import dao.TrendDAO;
import dao.TweetDAO;
import database.MySQL_UI;
import database.SqlConstants;
import domain.TwitNewsTrend;
import domain.TwitNewsTweet;
import domain.TwitNewsUser;
import domain.TwitNewsUserFactory;
import twitter4j.*;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import static util.TwitNewsConstants.US_WOEID;
import static util.TwitNewsConstants.WORLD_WOEID;

/**
 * Crawler class for retrieving twitter data from twitter.com.
 *
 * @author Chris Moghbel (cmoghbel@cs.ucla.edu)
 */
public class Crawler {

  private static final Twitter twitter = TwitterFactory.getSingleton();
  private static final Map<String, TwitNewsUser> userCache = new HashMap<String, TwitNewsUser>();

  private static MySQL_UI sql;

  private static final Logger log = Logger.getLogger(Crawler.class.getName());

	/**
	 * Entry point for the crawler. Sets up necessary data structures and objects, parses arguments, and kicks off a
   * crawl.
	 *
	 * @param args The command line arguments.
	 */
	public static void main(String[] args) {

    setupLogging();
    setupRateLimitStatusListener();

    boolean getUSTrendsOnly = Boolean.parseBoolean(args[0]);

    sql = new MySQL_UI(SqlConstants.HOST, SqlConstants.PORT, SqlConstants.USER, SqlConstants.PASSWORD);

    if (getUSTrendsOnly) {
      log.info("Crawling trends for the US...");
      crawlTrends(US_WOEID);
    }
    else {
      log.info("Crawling worldwide trends...");
      crawlTrends(WORLD_WOEID);
    }
    log.info("Finished crawling recent trends!");
	}

  /**
   * Queries twitter and lists all the current, unique trends for the given location using the REST api.
   *
   * @param woeid The Yahoo woeid for the location to get trends for.
   *
   * @see "http://developer.yahoo.com/geo/geoplanet/guide/concepts.html"
   */
  private static void crawlTrends(int woeid) {

    boolean firstTime = true;

    // Local cache to avoid fetching information about the same trend multiple times
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

    for (Trend trend : trendsForLocation.getTrends()) {

      String trendName = trend.getName().trim().toLowerCase();
      TwitNewsTrend twitNewsTrend = new TwitNewsTrend();
      twitNewsTrend.setName(trend.getName());

      if (!trendNames.contains(trendName)) {

        trendNames.add(trendName);
        QueryResult queryResult = null;
        try {
          Query query = new Query(trend.getName());
          query.setPage(3);
          query.setRpp(50);
          queryResult = twitter.search(query);
        }
        catch (TwitterException e) {
          log.warning("There was an error retrieving the tweets for this trend. Continuing onto the next trend...");
          e.printStackTrace();
          continue;
        }

        List<Tweet> tweets = queryResult.getTweets();

        if (!tweets.isEmpty()) {

          TrendDAO trendDAO = new TrendDAO(sql);
          int trendId = trendDAO.insertTrend(twitNewsTrend);

          if (!firstTime) {
            hourLongSleep();
          }
          firstTime = false;

          for (Tweet tweet : tweets) {

            TwitNewsUser twitNewsUser = getUserInformation(tweet);

            Timestamp timestamp = new Timestamp(tweet.getCreatedAt().getTime());

            // Parse location information if available
            GeoLocation location = tweet.getGeoLocation();
            double latitude = -1.0;
            double longitude = -1.0;
            if (location != null) {
              latitude = location.getLatitude();
              longitude = location.getLongitude();
            }

            // Parse tweet text to determine various attributes
            String tweetText = tweet.getText();
            String[] tweetTokens = tweetText.split(" ");
            boolean hasLink = false;
            boolean isRetweet = false;
            String link = null;
            for (String token : tweetTokens) {
              if (token.startsWith("http://")) {
                hasLink = true;
                link = token.trim();
              }
              if (token.trim().equals("RT")) {
                isRetweet = true;
              }
            }

            // TODO Figure out if there is away to avoid this call to reduce rate limiting strain.
            // default info in case we can't get the number of retweets from Twitter
            int numRetweets = -1;
            try {
              ResponseList<Status> retweets = twitter.getRetweets(tweet.getId());
              numRetweets = retweets.size();
            }
            catch (TwitterException e) {
              System.err.println("There was an error retrieving the retweets from twitter.");
              e.printStackTrace();
            }

            TwitNewsTweet twitNewsTweet = new TwitNewsTweet();
            twitNewsTweet.setTweetId(tweet.getId());
            twitNewsTweet.setTrendId(trendId);
            twitNewsTweet.setText(tweet.getText());
            twitNewsTweet.setUserId(tweet.getFromUserId());
            twitNewsTweet.setUserName(tweet.getFromUser());
            twitNewsTweet.setVerifiedUser(twitNewsUser.isVerifiedUser());
            twitNewsTweet.setNumFollowers(twitNewsUser.getNumFollowers());
            twitNewsTweet.setTimestamp(timestamp);
            twitNewsTweet.setLocationName(tweet.getLocation());
            twitNewsTweet.setLatitude(latitude);
            twitNewsTweet.setLongitude(longitude);
            twitNewsTweet.setHasLink(hasLink);
            twitNewsTweet.setLink(link);
            twitNewsTweet.setRetweet(isRetweet);
            twitNewsTweet.setRetweetCount(numRetweets);

            TweetDAO tweetDAO = new TweetDAO(sql);
            int success = tweetDAO.insertTweet(twitNewsTweet, trendId);
            log.info(twitNewsTweet.toString());
          }
        }
      }
    }
  }

  /**
   * Method that sleeps for 1 hour, printing out a status every minute. Intended as a convenience method to help
   * avoid rate limiting.
   */
  private static void hourLongSleep() {
    log.info("Sleeping so we don't get rate limited...");
    for (int i  = 0; i < 60; ++i) {
      try {
        // sleep for 1 minute
        Thread.sleep(60000);
        log.info(i + " minutes slept (probably).");
      }
      catch (InterruptedException e) {
        log.severe("Interupted while waiting!");
        --i;
      }
      finally {
        if (i < -5) {
          log.severe("We're probably never going to break out of this sleeping loop, let's exit.");
          System.exit(1);
        }
      }
    }
  }

  /**
   * Queries twitter and extracts relevant information about the user who authored the given tweet.
   *
   * @param tweet The tweet from which author information is desired.
   * @return TwitNewsUser containing the relevant information about the tweet author.
   */
  private static TwitNewsUser getUserInformation(Tweet tweet) {
    TwitNewsUser twitNewsUser = null;
    if (userCache.containsKey(tweet.getFromUser())) {
      twitNewsUser = userCache.get(tweet.getFromUser());
    }
    else {
      try {
        User user = twitter.showUser(tweet.getFromUser());
        twitNewsUser = TwitNewsUserFactory.fromUser(user);
      }
      catch (TwitterException e) {
        log.warning("Error retrieving user information from twitter, returning default user info.");
        e.printStackTrace();
        twitNewsUser = new TwitNewsUser();
        twitNewsUser.setVerifiedUser(false);
        twitNewsUser.setNumFollowers(0);
        twitNewsUser.setUserName(tweet.getFromUser());
        userCache.put(tweet.getFromUser(), twitNewsUser);
      }
    }
    return twitNewsUser;
  }

  /**
   * Sets up logging for the crawler. INFO and above will be logged to out file as well as the console, while
   * SEVERE and up will be logged to err file.
   */
  private static void setupLogging() {
    try {
      FileHandler out = new FileHandler("twitnews_out_" + System.currentTimeMillis());
      FileHandler err = new FileHandler("twitnews_err_" + System.currentTimeMillis());

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
   * Sets up the rate limit status listener. When the account or ip becomes rate limited, the listener will attempt
   * to sleep the thread until the application is no longer rate limited.
   *
   * TODO Currently this method doesn't work, as it operates on a seperate thread.
   *
   * @see "https://dev.twitter.com/docs/rate-limiting"
   */
  private static void setupRateLimitStatusListener() {
    // Need to add rate limit status listener to throttle crawler when we hit the rate limit
    RateLimitStatusListener rateLimitStatusListener = new RateLimitStatusListener() {
      @Override
      public void onRateLimitStatus(RateLimitStatusEvent event) {}

      @Override
      public void onRateLimitReached(RateLimitStatusEvent event) {
        try {
          RateLimitStatus status = event.getRateLimitStatus();
          if (event.isAccountRateLimitStatus()) {
            log.warning("Account is rate limited!");
          }
          if (event.isIPRateLimitStatus()) {
            log.warning("IP is rate limited!");
          }
          Date date = new Date();
          log.warning("The time is now: " + new Date());
          log.warning("Attempting to wait until rate limit reset at " + status.getResetTime() + " (" +
              status.getSecondsUntilReset() + " seconds).");
          Thread.sleep(status.getSecondsUntilReset());
        }
        catch (InterruptedException e) {
          e.printStackTrace();
          log.severe("Failure to wait!");
        }
      }
    };
    twitter.addRateLimitStatusListener(rateLimitStatusListener);
  }
}