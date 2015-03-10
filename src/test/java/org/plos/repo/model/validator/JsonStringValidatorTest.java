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

package org.plos.repo.model.validator;

import org.junit.Test;
import org.plos.repo.models.validator.JsonStringValidator;
import org.plos.repo.models.validator.TimestampInputValidator;
import org.plos.repo.service.RepoException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Timestamp Input Validator Test
 */
public class JsonStringValidatorTest {

  private String VALID_JSON_STRING = "{ \"key\": \"obj1\", \"test\":\"blabla\"}";

  private String NOT_VALID_JSON_STRING = "{ \"key\": \"obj1\", \"versionChecksum\":\"dkasdny84923mkdnu914i21\",}";

  private JsonStringValidator jsonStringValidator = new JsonStringValidator();

  @Test
  public void validTimestampTest() throws RepoException {

    jsonStringValidator.validate(VALID_JSON_STRING, RepoException.Type.InvalidUserMetadataFormat);

  }

  @Test
  public void invalidTimestampTest(){

    try{
      jsonStringValidator.validate(NOT_VALID_JSON_STRING, RepoException.Type.InvalidUserMetadataFormat);
      fail("A repo exception was expected. ");
    } catch (RepoException re){
      assertEquals(re.getType(), RepoException.Type.InvalidUserMetadataFormat);
    }


  }

}
