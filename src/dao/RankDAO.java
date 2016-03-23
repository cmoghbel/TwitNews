package dao;

import database.MySQL_UI;
import domain.TwitNewsRank;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

/**
 * A class for inserting and retrieving ranking information from the database.
 *
 * @author Chris Moghbel(cmoghbel@cs.ucla.edu)
 * @author Lei (Ricky) Jin (rickyjin@cs.ucla.edu)
 */
public class RankDAO {

  MySQL_UI sql;

  /**
   * Construct a new RankDAO object.
   *
   * @param sql A {@link MySQL_UI} object.
   */
  public RankDAO(MySQL_UI sql) {
    this.sql = sql;
  }

  /**
   * Inserts rank information into the db.
   *
   * @param rank The {@link TwitNewsRank} representing the rank info.
   * @return The id of the inserted rank info.
   */
  public int insertRank(TwitNewsRank rank) {
    int lastId = -1;
    PreparedStatement statement = null;
    ResultSet result = null;
    try {
      String insertString = "INSERT IGNORE INTO " + sql.getRankTableName() + " (trendId, tweetId, rank) VALUES (?, ?, ?);";
      statement = sql.prepareStatement(insertString);
      statement.setInt(1, rank.getTrendId());
      statement.setInt(2, rank.getTweetId());
      statement.setInt(3, rank.getRank());      
      
      statement.execute();

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
   * Inserts a series of rank info into the db.
   *
   * @param ranks {@link List} of {@link TwitNewsRank} objects.
   * @return boolean indicating the success or failure of the operation.
   */
  public boolean insertRanks(List<TwitNewsRank> ranks) {
    boolean success = true;
    PreparedStatement statement = null;
    ResultSet result = null;
    try {
      String insertString = "INSERT INTO " + sql.getRankTableName() + " (trendId, tweetId, rank) VALUES (?, ?, ?)";
      statement = sql.prepareStatement(insertString);

      for (TwitNewsRank rank : ranks) {

        statement.setInt(1, rank.getTrendId());
        statement.setInt(2, rank.getTweetId());
        statement.setInt(3, rank.getRank());

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
      sql.cleanUp(statement, result);
    }
    return success;
  }

  /**
   * Close the connection to the backing {@link MySQL_UI object}.
   */
  public void close() {
    sql.closeConnection();
  }
}