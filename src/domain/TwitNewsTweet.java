package domain;

import java.sql.Timestamp;

/**
 * Domain object representing a tweet.
 *
 * @author Chris Moghbel (cmoghbel@cs.ucla.edu)
 * @author Lei (Ricky) Jin (rickyjin@cs.ucla.edu)
 */
public class TwitNewsTweet {

  public long tweetId;
  public int trendId;
  private String text;
  private long userId;
  private String userName;
  private boolean isVerifiedUser;
  private int numFollowers;
  private Timestamp timestamp;
  private String locationName;
  private double latitude;
  private double longitude;
  private boolean hasLink;
  private String link;
  private boolean isRetweet;
  private long retweetCount;
  private int numTrendsContained;
  private int tfidf;
  private int rank;

  public int getTfidf() {
    return tfidf;
  }

  public void setTfidf(int tfidf) {
    this.tfidf = tfidf;
  }

  public int getRank() {
    return rank;
  }

  public void setRank(int rank) {
    this.rank = rank;
  }

  public long getTweetId() {
    return tweetId;
  }

  public void setTweetId(long tweetId) {
    this.tweetId = tweetId;
  }

  public int getTrendId() {
    return trendId;
  }

  public void setTrendId(int trendId) {
    this.trendId = trendId;
  }

  public String getText() {
    return text;
  }

  public void setText(String text) {
    this.text = text;
  }

  public long getUserId() {
    return userId;
  }

  public void setUserId(long userId) {
    this.userId = userId;
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
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

  public Timestamp getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(Timestamp timestamp) {
    this.timestamp = timestamp;
  }

  public String getLocationName() {
    return locationName;
  }

  public void setLocationName(String locationName) {
    this.locationName = locationName;
  }

  public double getLatitude() {
    return latitude;
  }

  public void setLatitude(double latitude) {
    this.latitude = latitude;
  }

  public double getLongitude() {
    return longitude;
  }

  public void setLongitude(double longitude) {
    this.longitude = longitude;
  }

  public boolean isHasLink() {
    return hasLink;
  }

  public void setHasLink(boolean hasLink) {
    this.hasLink = hasLink;
  }

  public String getLink() {
    return link;
  }

  public void setLink(String link) {
    this.link = link;
  }

  public boolean isRetweet() {
    return isRetweet;
  }

  public void setRetweet(boolean retweet) {
    isRetweet = retweet;
  }

  public long getRetweetCount() {
    return retweetCount;
  }

  public void setRetweetCount(long retweetCount) {
    this.retweetCount = retweetCount;
  }

  public int getNumTrendsContained() {
    return numTrendsContained;
  }

  public void setNumTrendsContained(int numTrendsContained) {
    this.numTrendsContained = numTrendsContained;
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer();
    sb.append("TwitNewsTweet");
    sb.append("{tweetId=").append(tweetId);
    sb.append(", trendId=").append(trendId);
    sb.append(", text='").append(text).append('\'');
    sb.append(", userId=").append(userId);
    sb.append(", userName='").append(userName).append('\'');
    sb.append(", isVerifiedUser=").append(isVerifiedUser);
    sb.append(", numFollowers=").append(numFollowers);
    sb.append(", link='").append(link).append('\'');
    sb.append(", retweetCount=").append(retweetCount);
    sb.append(", numTrendsContained=").append(numTrendsContained);
    sb.append(", isRetweet=").append(isRetweet);
    sb.append(", hasLink=").append(hasLink);
    sb.append('}');
    return sb.toString();
  }
}
