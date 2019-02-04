/*
 * Copyright (c) 2014-2019 Public Library of Science
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

package org.plos.repo.service;

/**
 * RepoExceptions usually capture a user/client side error
 */
public class RepoException extends Exception {

  private static final long serialVersionUID = 4654104578364892572L;

  public enum Type {
    ServerError(0, "Server error"), // this message is not used
    ObjectNotFound(1, "Object not found"),
    BucketNotFound(2, "Bucket not found"),
    CollectionNotFound(3, "Collection not found"),
    ObjectCollectionNotFound(4, "One of the objects of the Collection was not found"),
    ObjectContentNotFound(5, "The content of the object was not found"),

    // user errors for entered parameters
    NoBucketEntered(10, "No bucket entered"),
    NoKeyEntered(11, "No object key entered"),
    NoVersionEntered(12, "No object version entered"),
    NoCreationMethodEntered(13, "No creation method entered"),
    InvalidCreationMethod(14, "Invalid creation method"),
    CouldNotParseTimestamp(15, "Could not parse timestamp"),
    InvalidOffset(16, "Invalid offset"),
    InvalidLimit(17, "Invalid limit"),
    IllegalBucketName(18, "Bucket name contains illegal characters"),
    ObjectDataEmpty(19, "Object data must be non-empty"),
    NoCollectionKeyEntered(20, "No collection key entered"),
    CantDeleteObjectActiveColl(21, "Can not delete an object that is contain in an active collection. "),
    CouldNotParseCreationDate(22, "Could not parse creation date"),
    NoFilterEntered(23, "At least one of the filters is required"),
    MoreThanOneTaggedCollection(24, "There are more than one collections with that tag. Please specify version or versionNumber. "),
    MoreThanOneTaggedObject(25, "There are more than one object with that tag. Please specify version or versionNumber. "),
    InvalidUserMetadataFormat(26, "The user metadata must be a valid json. "),
    InvalidUuid(27, "The uuid format is invalid. "),


    // user errors for system state
    CantDeleteNonEmptyBucket(30, "Can not delete bucket since it contains objects"),
    CantCreateNewObjectWithUsedKey(31, "Can not create an object with a key that already exists"),
    CantCreateVersionWithNoOrig(32, "Can not version an object that does not exist"),
    BucketAlreadyExists(33, "Bucket already exists"),
    CantCreateNewCollectionWithUsedKey(34, "Can not create a collection with a key that already exists"),
    CantCreateCollectionVersionWithNoOrig(35, "Can not version a collection that does not exist"),
    CantCreateCollectionWithNoObjects(36, "Can not create a collection that does not have objects"),
    ObjectFilePathMissing(37, "The file path object is missing"),
    // user error for missing file
    NoFileEntered(38, "No file data entered");

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


  private final Type repoExceptionType;

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
    repoExceptionType = (e instanceof RepoException) ? ((RepoException) e).getType() : Type.ServerError;
  }

  public RepoException(String message) {  // server errors only
    super(message);
    repoExceptionType = Type.ServerError;
  }

}
