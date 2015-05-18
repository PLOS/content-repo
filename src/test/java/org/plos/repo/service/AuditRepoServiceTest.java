package org.plos.repo.service;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.plos.repo.models.Audit;

import java.sql.SQLException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

/**
 * Created by lmasola on 14/04/15.
 */
public class AuditRepoServiceTest {

  private static final Integer OFFSET = 0;
  private static final Integer LIMIT = 3;
  private static String NAME_BUCKET1 = "b1";
  private static String NAME_BUCKET2 = "b2";
  private static Long SIZE_BUCKET1 = 123456l;
  private static Long SIZE_BUCKET2 = 654321l;

  @InjectMocks
  private AuditRepoService auditRepoService;

  @Mock
  private SqlService sqlService;

  @Mock
  private List<Audit> expectedAuditRecords;

  @Before
  public void setUp() {
    auditRepoService = new AuditRepoService();
    initMocks(this);
  }

  @Test
  public void getStatusHappyPathTest() throws RepoException, SQLException {

    doNothing().when(sqlService).getReadOnlyConnection();
    when(sqlService.listAuditRecords(OFFSET, LIMIT)).thenReturn(expectedAuditRecords);

    List<Audit> auditResults= auditRepoService.listAuditRecords(OFFSET, LIMIT);

    assertNotNull(auditResults);
    assertEquals(expectedAuditRecords, auditResults);

    verify(sqlService).getReadOnlyConnection();
    verify(sqlService).listAuditRecords(OFFSET, LIMIT);
  }


}
