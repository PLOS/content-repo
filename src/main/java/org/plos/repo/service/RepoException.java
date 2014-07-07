package org.plos.repo.service;

/**
 * RepoExceptions usually capture a user/client side error
 */
public class RepoException extends Exception {

  public enum Type {

    ServerError(0, "Server error"), // this message is not used
    ObjectNotFound(1, "Object not found"),
    BucketNotFound(2, "Bucket not found"),

    // user errors for entered parameters
    NoBucketEntered(10, "No bucket entered"),
    NoKeyEntered(11, "No object key entered"),
    NoVersionEntered(12, "No object version entered"),
    NoCreateFlagEntered(13, "No creation method entered"),
    InvalidCreationMethod(14, "Invalid creation method"),
    CouldNotParseTimestamp(15, "Could not parse timestamp"),
    InvalidOffset(16, "Invalid offset"),
    InvalidLimit(17, "Invalid limit"),
    IllegalBucketName(18, "Bucket name contains illegal characters"),
    ObjectDataEmpty(19, "Object data must be non-empty"),

    // user errors for system state
    CantDeleteNonEmptyBucket(20, "Can not delete bucket since it contains objects"),
    CantCreateNewObjectWithUsedKey(21, "Can not create an object with a key that already exists"),
    CantCreateVersionWithNoOrig(22, "Can not version an object that does not exist"),
    BucketAlreadyExists(23, "Bucket already exists");


    private final int value;
    private final String message;

    private Type(int value, String message) {
      this.value = value;
      this.message = message;
    }

    public String getMessage() {
      return message;
    }

    public int getValue() {
      return value;
    }
  }


  private Type repoExceptionType;

  public Type getType() {
    return repoExceptionType;
  }

  public RepoException(Type type) {
    //super();
    super(type.getMessage());
    repoExceptionType = type;
  }

  public RepoException(Exception e) {  // server errors only
    super(e);
    repoExceptionType = Type.ServerError;
  }

  public RepoException(String message) {  // server errors only
    super(message);
    repoExceptionType = Type.ServerError;
  }

}
