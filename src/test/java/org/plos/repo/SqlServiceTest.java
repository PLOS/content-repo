package org.plos.repo;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.plos.repo.service.MysqlService;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;

import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class SqlServiceTest {

  private DataSource mockDataSource;
  private Connection mockConnection;
  private MysqlService mysqlService;
  private MysqlService mysqlServiceSpy;
  private PreparedStatement mockPreparedStatement;

  @Before
  public void setup() throws Exception {

    mockConnection = Mockito.mock(Connection.class);
    mockPreparedStatement = Mockito.mock(PreparedStatement.class);

    when (mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);

    mockDataSource = Mockito.mock(DataSource.class);

    mysqlService = new MysqlService();

    when (mockDataSource.getConnection()).thenReturn(mockConnection);

    mysqlServiceSpy = spy(mysqlService);
  }

  @Test
  public void testMarkObjectDeleted() throws Exception {

    Assert.assertNotNull(mockDataSource.getConnection());

    willReturn(4).given(mysqlServiceSpy).getBucketId(anyString());

    mysqlServiceSpy.setDataSource(mockDataSource);

    mysqlServiceSpy.markObjectDeleted("key", "bucket", 0);

//    verify(mockPreparedStatement, times(1)).executeUpdate();
//    verify(mockPreparedStatement, times(3)).setInt(anyInt(), anyInt());


    // TODO: make request to in memory sql database
  }

  @Test
  public void testMarkObjectDeletedInvalidBucketName() throws Exception {

    willReturn(null).given(mysqlServiceSpy).getBucketId(anyString());

    Assert.assertEquals(0, mysqlServiceSpy.markObjectDeleted("key", "badBucket", 0));

  }
}
