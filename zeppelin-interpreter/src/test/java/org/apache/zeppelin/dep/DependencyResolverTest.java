/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.dep;

import org.apache.commons.io.FileUtils;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.eclipse.aether.RepositoryException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;


class DependencyResolverTest {
  private static DependencyResolver resolver;
  private static String testPath;
  private static File testCopyPath;
  private static File tmpDir;


  @BeforeAll
  static void setUp() throws Exception {
    tmpDir = new File(System.getProperty("java.io.tmpdir") + "/ZeppelinLTest_" +
        System.currentTimeMillis());
    testPath = tmpDir.getAbsolutePath() + "/test-repo";
    testCopyPath = new File(tmpDir, "test-copy-repo");
    resolver = new DependencyResolver(testPath, ZeppelinConfiguration.load());
  }

  @AfterAll
  static void tearDown() throws Exception {
    FileUtils.deleteDirectory(tmpDir);
  }


  @Test
  void testAddRepo() {
    int reposCnt = resolver.getRepos().size();
    resolver.addRepo("securecentral", "https://repo1.maven.org/maven2", false);
    assertEquals(reposCnt + 1, resolver.getRepos().size());
  }

  @Test
  void testDelRepo() {
    resolver.addRepo("securecentral", "https://repo1.maven.org/maven2", false);
    int reposCnt = resolver.getRepos().size();
    resolver.delRepo("securecentral");
    resolver.delRepo("badId");
    assertEquals(reposCnt - 1, resolver.getRepos().size());
  }

  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS)
  void testLoad() throws Exception {
    // basic load from Maven Central (default repo)
    resolver.load("com.databricks:spark-csv_2.10:1.3.0", testCopyPath);
    assertEquals(4, testCopyPath.list().length);
    FileUtils.cleanDirectory(testCopyPath);

    // load with exclusions parameter
    resolver.load("com.databricks:spark-csv_2.10:1.3.0",
        Collections.singletonList("org.scala-lang:scala-library"), testCopyPath);
    assertEquals(3, testCopyPath.list().length);
    FileUtils.cleanDirectory(testCopyPath);

    // load non-existent artifact -> RepositoryException
    assertThrows(RepositoryException.class, () ->
        resolver.load("com.agimatec:agimatec-validation:0.12.0", testCopyPath));
  }

  @Test
  void should_throw_exception_if_dependency_not_found() throws Exception {
    FileNotFoundException exception = assertThrows(FileNotFoundException.class, () -> {
      resolver.load("one.two:1.0", testCopyPath);
    });
    assertEquals("File system element for parameter 'source' does not exist: 'one.two:1.0'",
        exception.getMessage());
  }

}
