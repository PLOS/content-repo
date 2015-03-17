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

package org.plos.repo.models.validator;

import org.plos.repo.models.input.InputCollection;
import org.plos.repo.service.RepoException;

import javax.inject.Inject;

/**
 * Input Collection Validator. It validates the required fields.
 */
public class InputCollectionValidator {

  @Inject
  private TimestampInputValidator timestampValidator;

  public void validate(InputCollection collection) throws RepoException {
    if (collection.getKey() == null) {
      throw new RepoException(RepoException.Type.NoCollectionKeyEntered);
    }

    if (collection.getBucketName() == null) {
      throw new RepoException(RepoException.Type.NoBucketEntered);
    }

    timestampValidator.validate(collection.getTimestamp(), RepoException.Type.CouldNotParseTimestamp);
    timestampValidator.validate(collection.getCreationDateTime(), RepoException.Type.CouldNotParseCreationDate);

    if (collection.getObjects() == null || collection.getObjects().size() == 0) {
      throw new RepoException(RepoException.Type.CantCreateCollectionWithNoObjects);
    }
  }

}
