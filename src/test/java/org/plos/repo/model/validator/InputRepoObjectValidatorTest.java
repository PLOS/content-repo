/*
 * Copyright (c) 2017 Public Library of Science
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

  private List<InputObject> inputObjects = Arrays.asList(new InputObject("key", "sads123dsadas456"));

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
