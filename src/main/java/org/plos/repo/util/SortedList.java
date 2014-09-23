package org.plos.repo.util;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;

public class SortedList<T> extends LinkedList<T> {

  /**
   * Comparator used to sort the list.
   */
  private Comparator<? super T> comparator = null;
  /**
   * Construct a new instance with the list elements sorted in their
   * {@link java.lang.Comparable} natural ordering.
   */
  public SortedList() {
    super();
  }

  public SortedList(Comparator<? super T> comparator) {
    super();
    this.comparator = comparator;
  }

  /**
   * Add the given element <code>paramT</code> to the list. It is added in the corresponding
   * position in order to maintain the list sorted. Is uses the <code>comparator</code>
   * @param paramT parameter to be added in the list
   * @return a boolean value showing the result of the operation
   */
  @Override
  public boolean add(T paramT) {
    int insertionPoint = Collections.binarySearch(this, paramT, comparator);
    insertionPoint = (insertionPoint > -1) ? insertionPoint : (-insertionPoint) - 1;
    super.add(insertionPoint, paramT);
    return true;
  }

  /**
   * Adds all elements from the given collection <code>paramCollection</code> maintaining the
   * order
   * @param paramCollection collection from where to obtain the elements to be added
   * @return a boolean value showing the result of the operation
   */
  @Override
  public boolean addAll(Collection<? extends T> paramCollection){
    Boolean result = null;
    for (T paramT:paramCollection) {
      result |= add(paramT);
    }
    return result;
  }


}
