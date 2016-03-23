package datastructures;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Shahin
 * Date: 11/3/11
 * Time: 4:35 PM
 * To change this template use File | Settings | File Templates.
 */
public class InvertedIndex<T1, T2> {

  private Map<T1, Set<T2>> invertedIndex = new HashMap<T1, Set<T2>>();

  public int size() {
    return invertedIndex.size();
  }

  public boolean isEmpty() {
    return invertedIndex.isEmpty();
  }

  public boolean containsKey(T1 key) {
    return invertedIndex.containsKey(key);
  }

  public boolean containsValue(T2 value) {
    for (T1 key : invertedIndex.keySet()) {
      Set<T2> documents = invertedIndex.get(key);
      if (documents.contains(value)) {
        return true;
      }
    }
    return false;
  }

  public Set<T2> get(T1 key) {
    return invertedIndex.get(key);
  }

  public void put(T1 key, T2 value) {
    if (invertedIndex.containsKey(key)) {
      Set<T2> c = invertedIndex.get(key);
      c.add(value);
    }
    else {
      Set <T2> c = new LinkedHashSet<T2>();
      c.add(value);
      invertedIndex.put(key, c);
    }
  }

  public void putAll(Collection<T1> keys, T2 value) {
    for (T1 key : keys) {
      put(key, value);
    }
  }

  public Set<T2> remove(T1 key) {
    return invertedIndex.remove(key);
  }

  public void clear() {
    invertedIndex.clear();
  }

  public Set<T1> keySet() {
    return invertedIndex.keySet();  //To change body of implemented methods use File | Settings | File Templates.
  }

  public Collection<T2> values() {
    Collection<T2> c = new LinkedHashSet<T2>();
    for (Set<T2> documents : invertedIndex.values()) {
      c.addAll(documents);
    }
    return c;
  }

}
