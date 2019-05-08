/*
 * Copyright (c) 2019 Public Library of Science
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

package org.plos.repo.service;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

import java.io.File;

import com.guba.mogilefs.MogileException;
import com.guba.mogilefs.MogileFS;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.plos.repo.TestSpringConfig;
import org.plos.repo.service.ObjectStore.UploadInfo;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestSpringConfig.class)
public class MogileStoreServiceTest {

  @Mock
  MogileFS mfs;

  @InjectMocks
  private MogileStoreService mogileStoreService;

  @Before
  public void setUp() throws Exception {
    String domain = "example";
    String[] trackerStrings = new String[]{"foo.example.com:7001", "bar.example.com:7001"};
    mogileStoreService = new MogileStoreService(domain, trackerStrings, 1, 1, 1000);
    initMocks(this);
  }
  
  @Test
  public void myTest() throws RepoException, MogileException {
    UploadInfo uploadInfo = mogileStoreService.uploadTempObject(IOUtils.toInputStream("foo"));

    assertEquals(new Long(3), uploadInfo.getSize());
    assertEquals("0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33", uploadInfo.getChecksum());
    verify(mfs).storeFile(eq(uploadInfo.getTempLocation()), eq(""), any(File.class));
  }
}
