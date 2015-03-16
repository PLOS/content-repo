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

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.plos.repo.models.input.InputObject;
import org.plos.repo.models.input.InputRepoObject;
import org.plos.repo.models.validator.InputRepoObjectValidator;
import org.plos.repo.models.validator.TimestampInputValidator;
import org.plos.repo.service.RepoException;

import java.util.Arrays;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

/**
 * Input Collection Validator test.
 */
public class InputRepoObjectValidatorTest {

  private static final String VALID_KEY = "valid-key";
  private static final String VALID_BUCKET_NAME = "valid-bucket-name";
  private static final String VALID_TIMESTAMP = "2014-09-02 1:55:32";
  private static final String FAIL_MSG = "A repo exception was expected.";

  @InjectMocks
  private InputRepoObjectValidator inputRepoObjectValidator;

  @Mock
  private TimestampInputValidator timestampInputValidator;

  @Mock
  private InputRepoObject inputRepoObject;

  private List<InputObject> inputObjects = Arrays.asList(new InputObject[]{new InputObject("key", "sads123dsadas456")});

  @Before
  public void setUp() {
    inputRepoObjectValidator = new InputRepoObjectValidator();
    initMocks(this);
  }

  @Test
  public void validateHappyPathTest() throws RepoException {

    mockInputCollectionCalls(inputObjects);

    inputRepoObjectValidator.validate(inputRepoObject);

    verifyInputCollectionCalls(2);

  }

  @Test
  public void validateNoKeyTest() throws RepoException {

    try {
      inputRepoObjectValidator.validate(inputRepoObject);
      fail(FAIL_MSG);
    } catch (RepoException re) {
      assertEquals(re.getType(), RepoException.Type.NoKeyEntered);
      verify(inputRepoObject).getKey();
    }

  }

  @Test
  public void validateNoBucketNameTest() throws RepoException {

    when(inputRepoObject.getKey()).thenReturn(VALID_KEY);
    try {
      inputRepoObjectValidator.validate(inputRepoObject);
      fail(FAIL_MSG);
    } catch (RepoException re) {
      assertEquals(re.getType(), RepoException.Type.NoBucketEntered);
      verify(inputRepoObject).getKey();
    }

  }

  private void mockInputCollectionCalls(List<InputObject> objects) throws RepoException {
    when(inputRepoObject.getKey()).thenReturn(VALID_KEY);
    when(inputRepoObject.getBucketName()).thenReturn(VALID_BUCKET_NAME);
    when(inputRepoObject.getTimestamp()).thenReturn(VALID_TIMESTAMP);
    when(inputRepoObject.getCreationDateTime()).thenReturn(VALID_TIMESTAMP);

    doNothing().when(timestampInputValidator).validate(VALID_TIMESTAMP, RepoException.Type.CouldNotParseTimestamp);
    doNothing().when(timestampInputValidator).validate(VALID_TIMESTAMP, RepoException.Type.CouldNotParseCreationDate);

  }

  private void verifyInputCollectionCalls(Integer getObjectsCalls) throws RepoException {
    verify(inputRepoObject).getKey();
    verify(inputRepoObject).getKey();
    verify(inputRepoObject).getBucketName();
    verify(inputRepoObject).getTimestamp();
    verify(inputRepoObject).getCreationDateTime();

    verify(timestampInputValidator).validate(VALID_TIMESTAMP, RepoException.Type.CouldNotParseTimestamp);
    verify(timestampInputValidator).validate(VALID_TIMESTAMP, RepoException.Type.CouldNotParseCreationDate);

  }


}
