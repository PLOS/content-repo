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

package org.plos.repo.model.validator;

import org.junit.Test;
import org.plos.repo.models.validator.TimestampInputValidator;
import org.plos.repo.service.RepoException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Timestamp Input Validator Test
 */
public class TimestampInputValidatorTest {

  private static final String VALID_TIMESTAMP = "2014-09-02 1:55:32";
  private static final String INVALID_TIMESTAMP = "2014-09-02";

  private TimestampInputValidator timestampInputValidator = new TimestampInputValidator();

  @Test
  public void validTimestampTest() throws RepoException {

    timestampInputValidator.validate(VALID_TIMESTAMP, RepoException.Type.CouldNotParseTimestamp);

  }

  @Test
  public void invalidTimestampTest(){

    try{
      timestampInputValidator.validate(INVALID_TIMESTAMP, RepoException.Type.CouldNotParseTimestamp);
      fail("A repo exception was expected. ");
    } catch (RepoException re){
      assertEquals(re.getType(), RepoException.Type.CouldNotParseTimestamp);
    }


  }

}
