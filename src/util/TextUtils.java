package util;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: Shahin
 * Date: 11/3/11
 * Time: 4:17 PM
 * To change this template use File | Settings | File Templates.
 */
public class TextUtils {

  public static Set<String> parseKeywordsFromTweetText(String text, Set<String> stopWords) {
    Set<String> keywords = new LinkedHashSet<String>();
    String[] words = text.split(" ");
    for (String word : words) {
      // Don't consider usernames
      if (!word.startsWith("@") && word.length() > 0) {
        // strip out non-alphanumeric characters
        String cleanedWord = word.replaceAll("[^\\p{L}\\p{N}]", "");
        // Make sure the word is not a stop word
        if (!(stopWords.contains(word.trim().toLowerCase()) || stopWords.contains(cleanedWord.trim().toLowerCase()))) {
          // if a word is capitalized, add it to the phrase
          if (cleanedWord.length() > 0 && Character.isUpperCase(cleanedWord.charAt(0))) {
            keywords.add(cleanedWord);
          }
        }
      }
    }
    return keywords;
  }

}
