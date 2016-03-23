package domain;

/**
 * Domain object representing a Trend. Should contain all the information necessary to fill in a record in the trends
 * table in the database.
 *
 * @author Chris Moghbel (cmoghbel@cs.ucla.edu)
 */
public class TwitNewsTrend {

  private int trendId;
  private String name;

  public int getTrendId() {
    return trendId;
  }

  public void setTrendId(int trendId) {
    this.trendId = trendId;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TwitNewsTrend that = (TwitNewsTrend) o;

    if (trendId != that.trendId) {
      return false;
    }
    if (name != null ? !name.equals(that.name) : that.name != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = trendId;
    result = 31 * result + (name != null ? name.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer();
    sb.append("TwitNewsTrend");
    sb.append("{trendId=").append(trendId);
    sb.append(", name='").append(name).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
