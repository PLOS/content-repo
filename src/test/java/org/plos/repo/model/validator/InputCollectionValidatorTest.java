package org.plos.repo.model.validator;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.plos.repo.models.InputCollection;
import org.plos.repo.models.InputObject;
import org.plos.repo.models.validator.InputCollectionValidator;
import org.plos.repo.models.validator.TimestampInputValidator;
import org.plos.repo.service.RepoException;

import java.util.Arrays;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

/**
 * Input Collection Validator test.
 */
public class InputCollectionValidatorTest {

  private static final String VALID_KEY = "valid-key";
  private static final String VALID_BUCKET_NAME = "valid-bucket-name";
  private static final String VALID_TIMESTAMP = "2014-09-02 1:55:32";
  private static final String FAIL_MSG = "A repo exception was expected.";

  @InjectMocks
  private InputCollectionValidator inputCollectionValidator;

  @Mock
  private TimestampInputValidator timestampInputValidator;

  @Mock
  private InputCollection inputCollection;

  private List<InputObject> inputObjects = Arrays.asList(new InputObject[]{new InputObject("key", "123456")});

  @Before
  public void setUp(){
    inputCollectionValidator = new InputCollectionValidator();
    initMocks(this);
  }

  @Test
  public void validateHappyPathTest() throws RepoException {

    mockInputCollectionCalls(inputObjects);

    inputCollectionValidator.validate(inputCollection);

    verifyInputCollectionCalls(2);

  }

  @Test
  public void validateNoKeyTest() throws RepoException {

    try{
      inputCollectionValidator.validate(inputCollection);
      fail(FAIL_MSG);
    } catch(RepoException re){
      assertEquals(re.getType(), RepoException.Type.NoCollectionKeyEntered);
      verify(inputCollection).getKey();
    }

  }

  @Test
  public void validateNoBucketNameTest() throws RepoException {

    when(inputCollection.getKey()).thenReturn(VALID_KEY);
    try{
      inputCollectionValidator.validate(inputCollection);
      fail(FAIL_MSG);
    } catch(RepoException re){
      assertEquals(re.getType(), RepoException.Type.NoBucketEntered);
      verify(inputCollection).getKey();
    }

  }

  @Test
  public void validateNoObjectsTest() throws RepoException {
    mockInputCollectionCalls(null);
    try{
      inputCollectionValidator.validate(inputCollection);
      fail(FAIL_MSG);
    } catch(RepoException re){
      assertEquals(re.getType(), RepoException.Type.CantCreateCollectionWithNoObjects);
      verifyInputCollectionCalls(1);
    }

  }

  private void mockInputCollectionCalls(List<InputObject> objects) throws RepoException {
    when(inputCollection.getKey()).thenReturn(VALID_KEY);
    when(inputCollection.getBucketName()).thenReturn(VALID_BUCKET_NAME);
    when(inputCollection.getTimestamp()).thenReturn(VALID_TIMESTAMP);
    when(inputCollection.getCreationDateTime()).thenReturn(VALID_TIMESTAMP);
    doNothing().when(timestampInputValidator).validate(VALID_TIMESTAMP, RepoException.Type.CouldNotParseTimestamp);
    doNothing().when(timestampInputValidator).validate(VALID_TIMESTAMP, RepoException.Type.CouldNotParseCreationDate);
    when(inputCollection.getObjects()).thenReturn(objects);
  }

  private void verifyInputCollectionCalls(Integer getObjectsCalls) throws RepoException{
    verify(inputCollection).getKey();
    verify(inputCollection).getKey();
    verify(inputCollection).getBucketName();
    verify(inputCollection).getTimestamp();
    verify(inputCollection).getCreationDateTime();
    verify(timestampInputValidator).validate(VALID_TIMESTAMP, RepoException.Type.CouldNotParseTimestamp);
    verify(timestampInputValidator).validate(VALID_TIMESTAMP, RepoException.Type.CouldNotParseCreationDate);
    verify(inputCollection, times(getObjectsCalls)).getObjects();
  }


}
