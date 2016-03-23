package dao;

import database.MySQL_UI;
import domain.TwitNewsTrend;
import domain.TwitNewsTrendFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A class for persisting trends to the database.
 *
 * @author Chris Moghbel
 */
public class TrendDAO {

  private MySQL_UI sql;

  public TrendDAO(MySQL_UI sql) {
    this.sql = sql;
  }

  public TwitNewsTrend fetchTrend(int trendId) {
    PreparedStatement statement;
    TwitNewsTrend trend = null;
    try {
      String queryString = "SELECT * FROM " + sql.getTrendTableName() + "WHERE trendId=?";
      statement = sql.prepareStatement(queryString);
      statement.setInt(1, trendId);
      ResultSet resultSet = statement.executeQuery();

      if (resultSet != null) {
        if(resultSet.next()) {
          trend = TwitNewsTrendFactory.fromResultSet(resultSet);
        }
      }
    }
    catch (SQLException e) {
      e.printStackTrace();
    }
    return trend;
  }

  public TwitNewsTrend fetchTrend(String trendName) {
    PreparedStatement statement;
    TwitNewsTrend trend = null;
    try {
      String queryString = "SELECT * FROM " + sql.getTrendTableName() + "WHERE trendName=?";
      statement = sql.prepareStatement(queryString);
      statement.setString(1, trendName);
      ResultSet resultSet = statement.executeQuery();

      if (resultSet != null) {
        if(resultSet.next()) {
          trend = TwitNewsTrendFactory.fromResultSet(resultSet);
        }
      }
    }
    catch (SQLException e) {
      e.printStackTrace();
    }
    return trend;
  }

  public List<TwitNewsTrend> fetchAllTrends() {
    PreparedStatement statement;
    List<TwitNewsTrend> trends = new ArrayList<TwitNewsTrend>();
    try {
      String queryString = "SELECT * FROM " + sql.getTrendTableName();
      statement = sql.prepareStatement(queryString);
      ResultSet resultSet = statement.executeQuery();

      if (resultSet != null) {
        while(resultSet.next()) {
          TwitNewsTrend trend = TwitNewsTrendFactory.fromResultSet(resultSet);
          trends.add(trend);
        }
      }
    }
    catch (SQLException e) {
      e.printStackTrace();
    }
    return trends;
  }

 /**
   * Tries to insert a single Twitter trend record.
   *
   * @param trendRecord trend record
   *
   * @return trendId, -1 if something went wrong
   */
  public int insertTrend(TwitNewsTrend trendRecord) {
    int lastId = -1;
    PreparedStatement statement = null;
    ResultSet result = null;
    try {
      String insertString = "INSERT IGNORE INTO " + sql.getTrendTableName() + " (trendName) VALUES (?)";
      statement = sql.prepareStatement(insertString);
      statement.setString(1, trendRecord.getName());

      statement.execute();

      /* get return value */
      statement = sql.prepareStatement("SELECT LAST_INSERT_ID();");
      result = statement.executeQuery();

      if(result != null){
        result.next();
        lastId = result.getInt(1);
      }

      // If we ignored, we have to manually get the id for the matching row.
      if (lastId == 0) {
        result.close();
        statement = sql.prepareStatement("SELECT trendId FROM " + sql.getTrendTableName() + " WHERE trendName=?");
        statement.setString(1, trendRecord.getName());
        result = statement.executeQuery();
        if (result != null) {
          result.next();
          lastId = result.getInt(1);
        }
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
   * Inserts a list of trends into the database via a batch insert.
   *
   * @param trends {@link java.util.Collection} of trends to insert
   * @return boolean indicating the success of the sql query.
   */
  public boolean insertTrends(Collection<TwitNewsTrend> trends) {
    boolean success = true;
    PreparedStatement statement = null;
    try {
      String insertString = "INSERT IGNORE INTO " + sql.getTrendTableName() + " (trendName) VALUES (?)";
      statement = sql.prepareStatement(insertString);

      for (TwitNewsTrend trend : trends) {
        statement.setString(1, trend.getName());
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

  public void close() {
    sql.closeConnection();
  }
}
