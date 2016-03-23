package dao;

import database.MySQL_UI;
import domain.TwitNewsUser;
import domain.TwitNewsUserFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Shahin
 * Date: 10/27/11
 * Time: 4:50 PM
 * To change this template use File | Settings | File Templates.
 */
public class UserDAO {

  private MySQL_UI sql;

  public UserDAO(MySQL_UI sql) {
    this.sql = sql;
  }

  public TwitNewsUser fetchUser(int userId) {
    PreparedStatement statement;
    TwitNewsUser user = null;
    try {
      String queryString = "SELECT * FROM " + sql.getUserTableName() + "WHERE userId=?";
      statement = sql.prepareStatement(queryString);
      statement.setInt(1, userId);
      ResultSet resultSet = statement.executeQuery();

      if (resultSet != null) {
        if(resultSet.next()) {
          user = TwitNewsUserFactory.fromResultSet(resultSet);
        }
      }
    }
    catch (SQLException e) {
      e.printStackTrace();
    }
    return user;
  }

  public List<TwitNewsUser> fetchUsers() {
    PreparedStatement statement;
    List<TwitNewsUser> users = new ArrayList<TwitNewsUser>();
    try {
      String queryString = "SELECT * FROM " + sql.getUserTableName();
      statement = sql.prepareStatement(queryString);
      ResultSet resultSet = statement.executeQuery();

      if (resultSet != null) {
        while(resultSet.next()) {
          TwitNewsUser user = TwitNewsUserFactory.fromResultSet(resultSet);
          users.add(user);
        }
      }
    }
    catch (SQLException e) {
      e.printStackTrace();
    }
    return users;
  }

  /**
   * Tries to insert a single Twitter user record.
   *
   * @param userRecord user record
   *
   * @return userId, -1 if anything went wrong
   */
  public int insertUser(TwitNewsUser userRecord) {
    int lastId = -1;
    PreparedStatement statement = null;
    ResultSet result = null;
    try {
      String insertString = "INSERT IGNORE INTO " + sql.getUserTableName() + " (userName, name, isVerifiedUser, numFollowers) " +
                            "VALUES (?, ?, ?, ?)";

      statement = sql.prepareStatement(insertString);
      statement.setString(1, userRecord.getUserName());
      statement.setString(2, userRecord.getName());
      statement.setBoolean(3, userRecord.isVerifiedUser());
      statement.setInt(4, userRecord.getNumFollowers());

      statement.executeUpdate();

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
        statement = sql.prepareStatement("SELECT userId FROM " + sql.getUserTableName() + "WHERE trendName=?");
        statement.setString(1, userRecord.getUserName());
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
   * Inserts a list of users into the db, via a batch insert.
   *
   * @param users A {@link java.util.Collection} of users to insert into the db.
   * @return boolean indicating the success or failure of the sql query.
   */
  public boolean insertUsers(Collection<TwitNewsUser> users) {
    boolean success = true;
    PreparedStatement statement = null;
    try {
      String insertString = "INSERT IGNORE INTO " + sql.getUserTableName() + " (userName, name, isVerifiedUser, numFollowers) " +
                            "VALUES (?, ?, ?, ?)";

      statement = sql.prepareStatement(insertString);
      for (TwitNewsUser user : users) {
        statement.setString(1, user.getUserName());
        statement.setString(2, user.getName());
        statement.setBoolean(3, user.isVerifiedUser());
        statement.setInt(4, user.getNumFollowers());

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
