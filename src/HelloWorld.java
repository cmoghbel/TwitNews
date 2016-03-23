import twitter4j.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.*;

/**
 * HelloWorld class demonstrating basic usage of twitter4j library for the purposes of
 * starting development on TwitNews project for cs246 at UCLA.
 * 
 * @author Chris Moghbel (cmoghbel@cs.ucla.edu)
 */
public class HelloWorld {

	/**
	 * Queries twitter and lists all the current, unique trends worldwide.
	 * 
	 * @param args
	 */
	public static void main(String[] args) throws IOException {
		System.out.println("Hello world! Here are the unique trends as of right now:");
		System.out.println("--------------------------------------------------------");
		System.out.println();
		
		Twitter twitter = TwitterFactory.getSingleton();

    Map<Trend, List<Tweet>> trendMap = new HashMap<Trend, List<Tweet>>();

    File outputFile = new File("twitnews_output");
    Writer writer = null;
		try {
      writer = new FileWriter(outputFile);
			ResponseList<Location> locations = twitter.getAvailableTrends();
			for (Location location : locations) {
				Trends trendsForLocation = twitter.getLocationTrends(location.getWoeid());
				for (Trend trend : trendsForLocation.getTrends()) {
          writer.write(trend.getName() + "\n");
          System.out.println(trend.getName());
          writer.write("------------------------------------------------------------------------------------------\n");
          System.out.println("------------------------------------------------------------------------------------------\n");
          QueryResult queryResult = twitter.search(new Query(trend.getQuery()));
          List<Tweet> tweets = queryResult.getTweets();
          trendMap.put(trend, tweets);
          for (Tweet tweet : tweets) {
            String tweetString = tweet.getId() + "," +
                                 tweet.getText() + "," +
                                 tweet.getFromUserId() + "," +
                                 tweet.getFromUser() + "," +
                                 tweet.getToUserId() + "," +
                                 tweet.getToUser() + "," +
                                 tweet.getSource();
            writer.write(tweetString + "\n");
            System.out.println(tweetString);
          }
          writer.write("\n");
          System.out.println("\n");
				}
				System.out.println();
			}
		}
    catch (TwitterException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    catch (IOException e) {
      e.printStackTrace();
    }
    finally {
      writer.close();
    }
	}

}
