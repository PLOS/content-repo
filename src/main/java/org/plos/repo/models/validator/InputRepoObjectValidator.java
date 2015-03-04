/*
 * Copyright (c) 2006-2015 by Public Library of Science
 *
 *    http://plos.org
 *    http://ambraproject.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.plos.repo.models.validator;

import org.plos.repo.models.input.InputRepoObject;
import org.plos.repo.service.RepoException;

import javax.inject.Inject;
import java.util.Map;

/**
 * Input Collection Validator. It validates the required fields.
 */
public class InputRepoObjectValidator {

  @Inject
  private TimestampInputValidator timestampValidator;

  @Inject
  private JsonStringValidator jsonStringValidator;

  public void validate(InputRepoObject repoObject) throws RepoException {

    if (repoObject.getKey() == null)
      throw new RepoException(RepoException.Type.NoKeyEntered);

    if (repoObject.getBucketName() == null)
      throw new RepoException(RepoException.Type.NoBucketEntered);

    timestampValidator.validate(repoObject.getTimestamp(), RepoException.Type.CouldNotParseTimestamp);
    timestampValidator.validate(repoObject.getCreationDateTime(), RepoException.Type.CouldNotParseCreationDate);

    jsonStringValidator.validate(repoObject.getUserMetadata(), RepoException.Type.InvalidUserMetadataFormat);

  }

  public String getJsonUserMetadata(Map<String, String> userMetadata) throws RepoException {
    return jsonStringValidator.toJson(userMetadata);
  }
}
