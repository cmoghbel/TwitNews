package domain;

/**
 * A domain object for wrapping ranking information.
 *
 * @author Chris Moghbel (cmoghbel@cs.ucla.edu)
 * @author Lei (Ricky) Jin (rickyjin@cs.ucla.edu)
 */
public class TwitNewsRank {

  private int rankId;
  private int trendId;
  private int tweetId;
  private int rank;

  public int getRankId() {
    return rankId;
  }

  public void setRankId(int rankId) {
    this.rankId = rankId;
  }

  public int getTrendId() {
    return trendId;
  }

  public void setTrendId(int trendId) {
    this.trendId = trendId;
  }

  public int getTweetId() {
    return tweetId;
  }

  public void setTweetId(int tweetId) {
    this.tweetId = tweetId;
  }

  public int getRank() {
    return rank;
  }

  public void setRank(int rank) {
    this.rank = rank;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TwitNewsRank that = (TwitNewsRank) o;

    if (rank != that.rank) {
      return false;
    }
    if (rankId != that.rankId) {
      return false;
    }
    if (trendId != that.trendId) {
      return false;
    }
    if (tweetId != that.tweetId) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result;
    long temp;
    result = rankId;
    result = 31 * result + trendId;
    result = 31 * result + tweetId;
    temp = rank != +0.0d ? Double.doubleToLongBits(rank) : 0L;
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer();
    sb.append("TwitNewsRank");
    sb.append("{rankId=").append(rankId);
    sb.append(", trendId=").append(trendId);
    sb.append(", tweetId=").append(tweetId);
    sb.append(", rank=").append(rank);
    sb.append('}');
    return sb.toString();
  }
}
