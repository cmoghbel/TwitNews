package domain;

import twitter4j.Status;
import twitter4j.User;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Class for the creation of TwitNewsUser objects.
 *
 * @author Chris Moghbel (cmoghbel@cs.ucla.edu)
 */
public class TwitNewsUserFactory {

  public static TwitNewsUser fromResultSet(ResultSet resultSet) {
    TwitNewsUser user = new TwitNewsUser();
    try {
      user.setUserName(resultSet.getString("userName"));
      user.setName(resultSet.getString("name"));
      user.setVerifiedUser(resultSet.getBoolean("isVerifiedUser"));
      user.setNumFollowers(resultSet.getInt("numFollowers"));
    }
    catch (SQLException e) {
      e.printStackTrace();
    }
    return user;
  }

  /**
   * Creates a new {@link TwitNewsUser} object.
   *
   * @param user {@link User} object to create the new TwitNewsUser object from.
   * @return TwitNewsUser The new user.
   */
  public static TwitNewsUser fromUser(User user) {
    TwitNewsUser twitNewsUser = new TwitNewsUser();
    if (user != null) {
      twitNewsUser.setUserName(user.getScreenName());
      twitNewsUser.setName(user.getName());
      twitNewsUser.setVerifiedUser(user.isVerified());
      twitNewsUser.setNumFollowers(user.getFollowersCount());
    }
    return twitNewsUser;
  }

  /**
   * Creates a new {@link TwitNewsUser} object.
   *
   * @param status {@link Status} object to create the new TwitNewsUser object from.
   * @return TwitNewsUser The new user.
   */
  public static TwitNewsUser fromStatus(Status status) {
    TwitNewsUser twitNewsUser = new TwitNewsUser();
    User user = status.getUser();
    if (user != null) {
      twitNewsUser.setUserName(user.getScreenName());
      twitNewsUser.setName(user.getName());
      twitNewsUser.setVerifiedUser(user.isVerified());
      twitNewsUser.setNumFollowers(user.getFollowersCount());
    }
    return twitNewsUser;
  }
}
