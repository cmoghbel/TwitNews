package domain;

import twitter4j.Trend;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by IntelliJ IDEA.
 * User: Shahin
 * Date: 10/27/11
 * Time: 5:02 PM
 * To change this template use File | Settings | File Templates.
 */
public class TwitNewsTrendFactory {

  public static TwitNewsTrend fromTrend(Trend trend) {
    TwitNewsTrend twitNewsTrend = new TwitNewsTrend();
    twitNewsTrend.setName(trend.getName());
    return twitNewsTrend;
  }

  public static TwitNewsTrend fromResultSet(ResultSet resultSet) {
    TwitNewsTrend trend = new TwitNewsTrend();
    try {
      trend.setTrendId(resultSet.getInt(1));
      trend.setName(resultSet.getString(2));
    }
    catch (SQLException e) {
      e.printStackTrace();
    }
    return trend;
  }
}
