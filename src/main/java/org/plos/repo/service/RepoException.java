package org.plos.repo.service;

/**
 * RepoExceptions usually capture a user/client side error
 */
public class RepoException extends Exception {

  public enum Type {
    ServerError(0), ClientError(1), ItemNotFound(2);

    private final int value;

    private Type(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }
  }

  private Type repoExceptionType;

  public Type getType() {
    return repoExceptionType;
  }

  public RepoException(Type type, String message) {
    super(message);
    repoExceptionType = type;
  }

  public RepoException(Type type, Exception e) {
    super(e);
    repoExceptionType = type;
  }
}
