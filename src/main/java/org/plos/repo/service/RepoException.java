package org.plos.repo.service;

/**
 * RepoExceptions usually capture a user/client side error
 */
public class RepoException extends Exception {

  public RepoException(String message) {
    super(message);
  }
}
