package domain;

/**
 * Domain object representing a user.
 *
 * @author Chris Moghbel (cmoghbel@cs.ucla.edu)
 */
public class TwitNewsUser {

  private String userName;
  private String name;
  private boolean isVerifiedUser;
  private int numFollowers;

  public TwitNewsUser() {}

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public boolean isVerifiedUser() {
    return isVerifiedUser;
  }

  public void setVerifiedUser(boolean verifiedUser) {
    isVerifiedUser = verifiedUser;
  }

  public int getNumFollowers() {
    return numFollowers;
  }

  public void setNumFollowers(int numFollowers) {
    this.numFollowers = numFollowers;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TwitNewsUser that = (TwitNewsUser) o;

    if (isVerifiedUser != that.isVerifiedUser) {
      return false;
    }
    if (numFollowers != that.numFollowers) {
      return false;
    }
    if (userName != null ? !userName.equals(that.userName) : that.userName != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = userName != null ? userName.hashCode() : 0;
    result = 31 * result + (name != null ? name.hashCode() : 0);
    result = 31 * result + (isVerifiedUser ? 1 : 0);
    result = 31 * result + numFollowers;
    return result;
  }
}
