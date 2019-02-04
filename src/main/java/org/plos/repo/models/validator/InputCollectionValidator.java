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
