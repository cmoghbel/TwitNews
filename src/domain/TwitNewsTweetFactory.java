package domain;

import twitter4j.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * Factory class for the construction of TwitNewsTweet objects.
 *
 * @author Chris Moghbel (cmoghbel@cs.ucla.edu)
 */
public class TwitNewsTweetFactory {

  public static TwitNewsTweet fromResultSet(ResultSet resultSet) {
    TwitNewsTweet tweet = new TwitNewsTweet();
    try {
      tweet.setTweetId(resultSet.getLong("tweetId"));
      tweet.setTrendId(resultSet.getInt("trendId"));
      tweet.setText(resultSet.getString("text"));
      tweet.setUserName(resultSet.getString("userName"));
      tweet.setVerifiedUser(resultSet.getBoolean("isVerifiedUser"));
      tweet.setNumFollowers(resultSet.getInt("numFollowers"));
      tweet.setTimestamp(resultSet.getTimestamp("timestamp"));
      tweet.setLocationName(resultSet.getString("locationName"));
      tweet.setLatitude(resultSet.getDouble("latitude"));
      tweet.setLongitude(resultSet.getDouble("longitude"));
      tweet.setHasLink(resultSet.getBoolean("hasLink"));
      tweet.setLink(resultSet.getString("link"));
      tweet.setRetweet(resultSet.getBoolean("isRetweet"));
      tweet.setRetweetCount(resultSet.getLong("retweetCount"));
      tweet.setTfidf(resultSet.getInt("tfidf"));
      tweet.setRank(resultSet.getInt("rank"));
    }
    catch (SQLException e) {
      e.printStackTrace();
    }
    return tweet;
  }

  /**
   * Creates a new {@link TwitNewsTweet} object from a given status object and associated trend id.
   *
   * @param status The {@link Status} object to create the TwitNewsTweet from.
   * @param trendId The id of the trend this tweet is associated with.
   * @return A newly created TwitNewsTweet object.
   */
  public static TwitNewsTweet fromStatus(Status status, int trendId) {
    TwitNewsTweet twitNewsTweet = new TwitNewsTweet();
    twitNewsTweet.setTweetId(status.getId());
    twitNewsTweet.setTweetId(trendId);
    twitNewsTweet.setText(status.getText());

    User user = status.getUser();
    twitNewsTweet.setUserId(user.getId());
    twitNewsTweet.setUserName(user.getScreenName());
    twitNewsTweet.setVerifiedUser(user.isVerified());
    twitNewsTweet.setNumFollowers(user.getFollowersCount());

    Timestamp timestamp = new Timestamp(status.getCreatedAt().getTime());
    twitNewsTweet.setTimestamp(timestamp);

    Place place = status.getPlace();
    if (place != null) {
      twitNewsTweet.setLocationName(place.getName());
    }

    GeoLocation location = status.getGeoLocation();
    if (location != null) {
      twitNewsTweet.setLatitude(location.getLatitude());
      twitNewsTweet.setLongitude(location.getLongitude());
    }
    else {
      twitNewsTweet.setLatitude(-1.0);
      twitNewsTweet.setLongitude(-1.0);
    }

    // Try and grab the appropriate link.
    MediaEntity[] mediaEntities = status.getMediaEntities();
    URLEntity[] urls = status.getURLEntities();
    if (mediaEntities != null && mediaEntities.length > 0) {
      twitNewsTweet.setHasLink(true);
      twitNewsTweet.setLink(mediaEntities[0].getExpandedURL().toString());
    }
    else if (urls != null && urls.length > 0) {
      twitNewsTweet.setHasLink(true);
      if (urls[0].getExpandedURL() != null) {
        twitNewsTweet.setLink(urls[0].getExpandedURL().toString());
      }
      else {
        twitNewsTweet.setLink(urls[0].toString());
      }
    }
    else {
      twitNewsTweet.setHasLink(false);
      twitNewsTweet.setLink(null);
    }

    twitNewsTweet.setRetweet(status.isRetweet());
    twitNewsTweet.setRetweetCount(status.getRetweetCount());

    twitNewsTweet.setTfidf(0);
    twitNewsTweet.setRank(0);

    return twitNewsTweet;
  }
}
