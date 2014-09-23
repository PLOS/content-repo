package org.plos.repo.util;

import org.plos.repo.models.ElementType;
import org.plos.repo.models.Operation;

import java.util.Comparator;

/**
 * Represents an operation perform in the content repo, such as inserting or updating the DB
 */
public class OperationComparator implements Comparator<Operation> {

  @Override
  public int compare(Operation o1, Operation o2) {

    if (o1.getLastModification().before(o2.getLastModification())) {
      return -1;
    }
    if (o1.getLastModification().after(o2.getLastModification())) {
      return 1;
    }
    // if the two operations have the same timestamp, the order of the operations
    // must follow: BUCKETS, OBJECTS, COLLECTIONS
    if (o1.getElementType().equals(ElementType.BUCKET)){
      return -1;
    }
    if (o2.getElementType().equals(ElementType.BUCKET)){
      return 1;
    }
    if (o1.getElementType().equals(ElementType.OBJECT)){
      return -1;
    }
    if (o2.getElementType().equals(ElementType.OBJECT)){
      return 1;
    }
    if (o1.getElementType().equals(ElementType.COLLECTION)){
      return -1;
    }
    if (o2.getElementType().equals(ElementType.COLLECTION)){
      return 1;
    }
    return 0;
  }


}
