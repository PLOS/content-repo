/*
 * Copyright (c) 2006-2014 by Public Library of Science
 * http://plos.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
