package dao;

import database.MySQL_UI;
import domain.TwitNewsTweet;
import domain.TwitNewsTweetFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A class for persisting Tweets to the database.
 *
 * @author Chris Moghbel (cmoghbel@cs.ucla.edu)
 */
public class TweetDAO {

  private MySQL_UI sql;

  /**
   * Constructs a new TweetDAO object.
   *
   * @param sql {@link MySQL_UI} containing an open connection to the db.
   */
  public TweetDAO(MySQL_UI sql) {
    this.sql = sql;
  }

  /**
   * Retrieves an individual tweet from the db.
   *
   * @param tweetId The id of the tweet to fetch.
   * @return A new {@link TwitNewsTweet} object representing the tweet.
   */
  public TwitNewsTweet fetchTweet(int tweetId) {
    PreparedStatement statement;
    TwitNewsTweet tweet = null;
    try {
      String queryString = "SELECT * FROM " + sql.getTweetTableName() + "WHERE tweetId=?";
      statement = sql.prepareStatement(queryString);
      statement.setInt(1, tweetId);
      ResultSet resultSet = statement.executeQuery();

      if (resultSet != null) {
        if(resultSet.next()) {
          tweet = TwitNewsTweetFactory.fromResultSet(resultSet);
        }
      }
    }
    catch (SQLException e) {
      e.printStackTrace();
    }
    return tweet;
  }

  /**
   * Fetches a list of tweets from the db.
   *
   * @param trendId The associated trendId of the tweets to fetch.
   * @return A {@link List} of {@link TwitNewsTweet} objects representing the tweets.
   */
  public List<TwitNewsTweet> fetchTweets(int trendId) {
    PreparedStatement statement = null;
    ResultSet resultSet = null;
    List<TwitNewsTweet> tweets = new ArrayList<TwitNewsTweet>();
    try {
      String queryString = "SELECT * FROM " + sql.getTweetTableName() + " WHERE trendId=?";
      statement = sql.prepareStatement(queryString);
      statement.setInt(1, trendId);
      resultSet = statement.executeQuery();

      if (resultSet != null) {
        while(resultSet.next()) {
          TwitNewsTweet tweet = TwitNewsTweetFactory.fromResultSet(resultSet);
          tweets.add(tweet);
        }
      }
    }
    catch (SQLException e) {
      e.printStackTrace();
    }
    finally {
      sql.cleanUp(statement, resultSet);
    }
    return tweets;
  }

  /**
   * Inserts a tweet into the db.
   *
   * @param tweet {@link domain.TwitNewsTweet} object to insert.
   * @param trendId int representing the associated trends id in the db
   * @return int the id of the item just inserted into the db, -1 if something went wrong.
   */
  public int insertTweet(TwitNewsTweet tweet, int trendId) {
    int lastId = -1;

    String insertString = "INSERT INTO " + sql.getTweetTableName() +
                         " (trendId, " +
                         " userName," +
                         " isVerifiedUser," +
                         " numFollowers," +
                         " text," +
                         " timestamp," +
                         " locationName," +
                         " latitude," +
                         " longitude," +
                         " hasLink," +
                         " link," +
                         " isRetweet," +
                         " retweetCount," +
                         " tfidf," +
                         " rank)" +
                         " VALUES " +
                         "(?," + // trendId
                         " ?," + // userName
                         " ?," + // isVerifiedUser
                         " ?," + // numFollowers
                         " ?," + // text
                         " ?," + // timestamp
                         " ?," + // locationName
                         " ?," + // latitude
                         " ?," + // longitude
                         " ?," + // hasLink
                         " ?," + // link
                         " ?," + // isRetweet
                         " ?," + // retweetCount
                         " ?," + // tfidf
                         " ?);"; // rank

    PreparedStatement statement = null;
    ResultSet result = null;
    try {
      statement = sql.prepareStatement(insertString);

      statement.setInt(1, trendId);
      statement.setString(2, tweet.getUserName());
      statement.setBoolean(3, tweet.isVerifiedUser());
      statement.setInt(4, tweet.getNumFollowers());
      statement.setString(5, tweet.getText());
      statement.setTimestamp(6, tweet.getTimestamp());
      statement.setString(7, tweet.getLocationName());
      statement.setDouble(8, tweet.getLatitude());
      statement.setDouble(9, tweet.getLongitude());
      statement.setBoolean(10, tweet.isHasLink());
      statement.setString(11, tweet.getLink());
      statement.setBoolean(12, tweet.isRetweet());
      statement.setLong(13, tweet.getRetweetCount());
      statement.setInt(14, tweet.getTfidf());
      statement.setInt(15, tweet.getRank());

      statement.executeUpdate();

      /* get return value */
      statement = sql.prepareStatement("SELECT LAST_INSERT_ID();");
      result = statement.executeQuery();

      if(result != null){
        result.next();
        lastId = result.getInt(1);
      }
    }
    catch (Exception e) {
      e.printStackTrace();
    }
    finally {
      sql.cleanUp(statement, result);
    }
    return lastId;
  }

  /**
   * Inserts a list of tweets to the db, via a batch insert.
   *
   * @param tweets {@link java.util.Collection} of {@link TwitNewsTweet} objects to persist to the db.
   * @param trendId int representing the associated trends id in the database.
   * @return boolean indicating the success of the sql query.
   */
  public boolean insertTweets(Collection<TwitNewsTweet> tweets, int trendId) {

    boolean success = true;

    String insertString = "INSERT INTO " + sql.getTweetTableName() +
                         " (trendId, " +
                         " userName," +
                         " isVerifiedUser," +
                         " numFollowers," +
                         " text," +
                         " timestamp," +
                         " locationName," +
                         " latitude," +
                         " longitude," +
                         " hasLink," +
                         " link," +
                         " isRetweet," +
                         " retweetCount," +
                         " tfidf," +
                         " rank)" +
                         " VALUES " +
                         "(?," + // trendId
                         " ?," + // userName
                         " ?," + // isVerifiedUser
                         " ?," + // numFollowers
                         " ?," + // text
                         " ?," + // timestamp
                         " ?," + // locationName
                         " ?," + // latitude
                         " ?," + // longitude
                         " ?," + // hasLink
                         " ?," + // link
                         " ?," + // isRetweet
                         " ?," + // retweetCount
                         " ?," + // tfidf
                         " ?);"; // rank

    PreparedStatement statement = null;
    try {
      statement = sql.prepareStatement(insertString);

      for (TwitNewsTweet tweetRecord : tweets) {

        statement.setInt(1, trendId);
        statement.setString(2, tweetRecord.getUserName());
        statement.setBoolean(3, tweetRecord.isVerifiedUser());
        statement.setInt(4, tweetRecord.getNumFollowers());
        statement.setString(5, tweetRecord.getText());
        statement.setTimestamp(6, tweetRecord.getTimestamp());
        statement.setString(7, tweetRecord.getLocationName());
        statement.setDouble(8, tweetRecord.getLatitude());
        statement.setDouble(9, tweetRecord.getLongitude());
        statement.setBoolean(10, tweetRecord.isHasLink());
        statement.setString(11, tweetRecord.getLink());
        statement.setBoolean(12, tweetRecord.isRetweet());
        statement.setLong(13, tweetRecord.getRetweetCount());
        statement.setInt(14, tweetRecord.getTfidf());
        statement.setInt(15, tweetRecord.getRank());

        statement.addBatch();
      }

      sql.setAutoCommit(false);
      statement.executeBatch();
      sql.setAutoCommit(true);
    }
    catch (Exception e) {
      e.printStackTrace();
      success = false;
    }
    finally {
      sql.cleanUp(statement, null);
    }
    return success;
  }


  public void updateRank(TwitNewsTweet tweet) {
    String queryString = "UPDATE " + sql.getTweetTableName() + " SET rank=? WHERE tweetId=?";

    PreparedStatement statement = null;
    try {
      statement = sql.prepareStatement(queryString);
      statement.setInt(1, tweet.getRank());
      statement.setLong(1, tweet.getTweetId());

      statement.executeUpdate();
    }
    catch (SQLException e) {
      e.printStackTrace();
    }
    finally {
      sql.cleanUp(statement, null);
    }
  }

  public void updateRanks(List<TwitNewsTweet> tweets) {
    String queryString = "UPDATE " + sql.getTweetTableName() + " SET rank=? WHERE tweetId=?";

    PreparedStatement statement = null;
    try {
      statement = sql.prepareStatement(queryString);

      for (TwitNewsTweet tweet : tweets) {
        statement.setInt(1, tweet.getRank());
        statement.setLong(2, tweet.getTweetId());

        statement.addBatch();
      }

      sql.setAutoCommit(false);
      statement.executeBatch();
      sql.setAutoCommit(true);
    }
    catch (SQLException e) {
      e.printStackTrace();
    }
    finally {
      sql.cleanUp(statement, null);
    }
  }

  /**
   * Close the underlying connection.
   */
  public void close() {
    sql.closeConnection();
  }
}
