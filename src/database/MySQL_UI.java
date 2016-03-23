package database;

import java.sql.*;

/**
 * Simple interface between Java and MySQL
 *
 * @author Lei (Ricky) Jin (rickyjin@cs.ucla.edu)
 * @author Chris Moghbel (cmoghbel@cs.ucla.edu)
 */
public class MySQL_UI {

  private final String SQL_HOST;
  private final String SQL_PORT;
  private final String SQL_USER;
  private final String SQL_PASSWORD;

  private Connection conn = null;
  
  private String twitterDB = "TweetDB";
  private String trendTable = "TweetDB.trends";
  private String userTable = "TweetDB.users";
  private String tweetTable = "TweetDB.tweets";
  private String rankTable = "TweetDB.ranks";

  /**
   * Create a new MySQL_UI instance.
   *
   * @param host Hostname.
   * @param port Port.
   * @param user Username.
   * @param password Password.
   */
  public MySQL_UI(String host, String port, String user, String password) {
    SQL_HOST = host;
    SQL_PORT = port;
    SQL_USER = user;
    SQL_PASSWORD = password;

    try {
      conn = getConnection(conn);
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  public String getDBName() {
    return twitterDB;
  }

  public void setDBName(String name) {
    this.twitterDB = name;
  }
  
  public String getTrendTableName() {
    return trendTable;
  }

  public void setTrendTableName(String name) {
    this.trendTable = name;
  }
  
  public String getUserTableName() {
    return userTable;
  }

  public void setUserTableName(String name) {
    this.userTable = name;
  }
  
  public String getTweetTableName() {
    return tweetTable;
  }

  public void setTweetTableName(String name) {
    this.tweetTable = name;
  }

  public String getRankTableName() {
    return rankTable;
  }

  public void setRankTableName(String name) {
    rankTable = name;
  }
  
  /**
   * Constructor/helper function to set up the database/tables
   */
  public void setupDatabase() {
    try {
      dropDatabase();
      createDatabase();
      createTrendTable();
      createUserTable();
      createTweetTable();
      createRankTable();
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  /**
   * Tries to create Twitter database.
   */
  public void createDatabase() {
    Statement query = null;
    try {
      /* create a query statement */
      query = conn.createStatement();

      /* execute the query */
      query.executeUpdate("CREATE DATABASE IF NOT EXISTS " + twitterDB + ";");
    }
    catch (Exception e) {
      e.printStackTrace();
    }
    finally {
      cleanUp(query, null);
    }
  }

  /**
   * Tries to create Twitter trend table.
   *
   * @return boolean indicating the success or failure of the sql query.
   */
  public boolean createTrendTable() {
    boolean success = true;
    Statement query = null;
    try {
      /* create a query statement */
      query = conn.createStatement();

      /* execute the query */
      query.executeUpdate("CREATE TABLE IF NOT EXISTS " + trendTable +
                          "(trendId     INT NOT NULL AUTO_INCREMENT," +
                          "             PRIMARY KEY(trendId)," +
                          " trendName   VARCHAR(140)," +
                          " UNIQUE(trendName)" +
                          ");" );
    }
    catch (Exception e) {
      e.printStackTrace();
      success = false;
    }
    finally {
      cleanUp(query, null);
    }
    return success;
  }
  
  /**
   * Tries to create Twitter user table
   *
   * @return boolean indicating the success of the sql query.
   */
  public boolean createUserTable() {
    boolean success = true;
    Statement query = null;
    try {
      /* create a query statement */
      query = conn.createStatement();

      /* execute the query */
      query.executeUpdate("CREATE TABLE IF NOT EXISTS " + userTable +
                          "(userId          INT NOT NULL AUTO_INCREMENT," +
                          "                 PRIMARY KEY(userId)," +
                          " userName        VARCHAR(140)," +
                          " name            VARCHAR(140)," +
                          " isVerifiedUser  BOOL," +
                          " numFollowers    INT," +
                          " UNIQUE(userName)" +
                          ");" );
    }
    catch (Exception e) {
      e.printStackTrace();
      success = false;
    }
    finally {
      cleanUp(query, null);
    }
    return success;
  }
  
  /**
   * Tries to create Twitter tweet table.
   *
   * @return boolean indicating the success of the sql query.
   */
  public boolean createTweetTable() {
    boolean success = true;
    Statement query = null;
    try {
      
      /* create a query statement */
      query = conn.createStatement();

      /* execute the query */
      query.executeUpdate("CREATE TABLE IF NOT EXISTS " + tweetTable +
                          "(tweetId         INT NOT NULL AUTO_INCREMENT," +
                          "                 PRIMARY KEY (tweetId)," +
                          " trendId         INT," +
                          " userName        VARCHAR(40)," +
                          " isVerifiedUser  BOOL," +
                          " numFollowers    INT," +
                          " text            VARCHAR(140)," +
                          " timestamp       TIMESTAMP," +
                          " locationName    VARCHAR(255)," +
                          " latitude        DECIMAL," +
                          " longitude       DECIMAL," +
                          " hasLink         BOOL," +
                          " link            VARCHAR(140)," +
                          " isRetweet       BOOL," +
                          " retweetCount    INT," +
                          " tfidf           Int," +
                          " rank            Int," +
                          " FOREIGN KEY (trendId)" +
                          "   REFERENCES TweetDB.trends(trendId)" +
                          "   " +
                          ");" );
    }
    catch (Exception e) {
      e.printStackTrace();
      success = false;
    }
    finally {
      cleanUp(query, null);
    }
    return success;
  }

  /**
   * Creates the rank table in the db.
   *
   * @return boolean indicating the success or failure of the operation.
   */
  public boolean createRankTable() {
    boolean  success = true;
    PreparedStatement statement = null;
    try {

      String queryString = "CREATE TABLE IF NOT EXISTS " + getRankTableName() +
                           "(rankId           INT NOT NULL AUTO_INCREMENT," +
                           "                  PRIMARY KEY (rankId)," +
                           " trendId          INT NOT NULL," +
                           " tweetId          INT NOT NULL," +
                           " rank             INT NOT NULL," +
                           " FOREIGN KEY (trendId)" +
                           "   REFERENCES TweetDB.trends(trendId)" +
                           "   ," +
                           " FOREIGN KEY (tweetId)" +
                           "   REFERENCES TweetDB.tweets(tweetId)" +
                           "   " +
                           ");";
      statement = conn.prepareStatement(queryString);
      statement.execute();
    }
    catch (SQLException e) {
      e.printStackTrace();
      success = false;
    }
    return success;
  }
  
  /**
   * Drops the entire database, useful for a quick reset.
   */
  public void dropDatabase() {
    Statement query = null;
    try {
      /* create a query statement */
      query = conn.createStatement();

      /* execute the query */
      query.executeUpdate("DROP DATABASE IF EXISTS " + twitterDB + ";");
    }
    catch (Exception e) {
      e.printStackTrace();
    }
    finally {
      cleanUp(query, null);
    }
  }

  /**
   * Creates a {@link PreparedStatement} from the backing connection.
   *
   * @param sqlString The string representing the sql query.
   * @return The new PerparedStatement object.
   * @throws SQLException If there was a problem creating the prepared statement.
   */
  public PreparedStatement prepareStatement(String sqlString) throws SQLException {
    return conn.prepareStatement(sqlString);
  }

  /**
   * Changes the state of the auto commmit feature of sql connection.
   *
   * @param state The new state.
   * @throws SQLException If there was a problem changing the state.
   */
  public void setAutoCommit(boolean state) throws SQLException {
    conn.setAutoCommit(state);
  }

  /**
   * Closes the connection for this instance.
   */
  public void closeConnection() {
    try {
      conn.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  /**
   * Tries to get connection to MySQL database.
   *
   * @param conn JDBC Connection object.
   * 
   * @return new JDBC Connection object.
   */
  private Connection getConnection(Connection conn) {
    try {
      String url = "jdbc:mysql://" + SQL_HOST + ":" + SQL_PORT + "/mysql";
      
      /* load the MySQL driver */
      Class.forName("com.mysql.jdbc.Driver");
      
      /* setup the connection with MySQL */
      conn = DriverManager.getConnection (url, SQL_USER, SQL_PASSWORD);
    }
    catch (Exception e) {
      e.printStackTrace();
    }
    return conn;
  }
  
  /**
   * Cleans up the variables.
   *
   * @param query The {@link Statement} object. Can be null.
   * @param result The {@link ResultSet} object. Can be null.
   */
  public void cleanUp(Statement query, ResultSet result) {
    try {
      if (result != null) {
        result.close();
      }

      if (query != null) {
        query.close();
      }
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  /**
   * Quick script to set up the blank database
   */
  public static void main(String[] args) throws Exception {
    System.out.println("Attempting to set up brand new database.");

    MySQL_UI sql = new MySQL_UI("localhost", "3309", "root", "sqladmin");
    sql.setupDatabase();

    System.out.println("Successfully created brand new database.");
  }
}