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

package org.apache.zeppelin.interpreter;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.display.AngularObjectRegistryListener;
import org.apache.zeppelin.helium.ApplicationEventListener;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreterProcessListener;
import org.apache.zeppelin.storage.ConfigStorage;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests for zeppelin.interpreter.json.preserve: an existing interpreter.json is used as is.
 */
class InterpreterSettingManagerPreserveTest extends AbstractInterpreterTest {

  // ids of settings in src/test/resources/conf/interpreter.json
  private static final String TEST_SETTING_ID = "2C3RWCVAG";   // name "test", 1 interpreter "echo"
  private static final String MOCK1_SETTING_ID = "2C4BJDRRZ";  // name "mock1", no interpreterGroup

  /**
   * Writes a copy of the test interpreter.json that keeps only the given settings,
   * and returns its bytes.
   */
  private byte[] writeInterpreterJson(String... settingIds) throws IOException {
    File file = new File(confDir, "interpreter.json");
    JsonObject root = JsonParser.parseString(
        new String(Files.readAllBytes(new File("src/test/resources/conf/interpreter.json").toPath()),
            StandardCharsets.UTF_8)).getAsJsonObject();
    JsonObject settings = root.getAsJsonObject("interpreterSettings");
    JsonObject kept = new JsonObject();
    for (String id : settingIds) {
      kept.add(id, settings.get(id));
    }
    root.add("interpreterSettings", kept);
    byte[] content = root.toString().getBytes(StandardCharsets.UTF_8);
    Files.write(file.toPath(), content);
    return content;
  }

  private byte[] readInterpreterJson() throws IOException {
    return Files.readAllBytes(new File(confDir, "interpreter.json").toPath());
  }

  private InterpreterSettingManager newManager(boolean preserve) throws IOException {
    zConf.setProperty(ZeppelinConfiguration.ConfVars.ZEPPELIN_INTERPRETER_JSON_PRESERVE.getVarName(),
        String.valueOf(preserve));
    return new InterpreterSettingManager(zConf,
        mock(AngularObjectRegistryListener.class), mock(RemoteInterpreterProcessListener.class),
        mock(ApplicationEventListener.class), ConfigStorage.createConfigStorage(zConf),
        pluginManager);
  }

  private static List<String> names(InterpreterSettingManager manager) {
    return manager.get().stream().map(InterpreterSetting::getName).sorted()
        .collect(Collectors.toList());
  }

  @Test
  void testSettingsFromTemplatesAreNotAdded() throws IOException {
    writeInterpreterJson(TEST_SETTING_ID);

    InterpreterSettingManager preserved = newManager(true);
    try {
      assertEquals(List.of("test"), names(preserved));
    } finally {
      preserved.close();
    }

    // without the flag the missing settings are created from the templates, as before
    writeInterpreterJson(TEST_SETTING_ID);
    InterpreterSettingManager notPreserved = newManager(false);
    try {
      assertTrue(names(notPreserved).size() > 1);
    } finally {
      notPreserved.close();
    }
  }

  @Test
  void testSavedInterpreterGroupIsKept() throws IOException {
    writeInterpreterJson(TEST_SETTING_ID);

    InterpreterSettingManager manager = newManager(true);
    try {
      List<InterpreterInfo> infos = manager.getByName("test").getInterpreterInfos();
      // the template of group "test" has 8 interpreters, the saved setting has only "echo"
      assertEquals(1, infos.size());
      assertEquals("echo", infos.get(0).getName());
    } finally {
      manager.close();
    }
  }

  @Test
  void testMissingInterpreterGroupFallsBackToTemplate() throws IOException {
    writeInterpreterJson(MOCK1_SETTING_ID);

    InterpreterSettingManager manager = newManager(true);
    try {
      List<InterpreterInfo> infos = manager.getByName("mock1").getInterpreterInfos();
      assertNotNull(infos);
      assertFalse(infos.isEmpty());
    } finally {
      manager.close();
    }
  }

  @Test
  void testDescriptionsAreNotFilledFromTemplate() throws IOException {
    writeInterpreterJson(TEST_SETTING_ID);

    InterpreterSettingManager manager = newManager(true);
    try {
      // the template has "desc_1" for property_1, the saved setting has no description
      @SuppressWarnings("unchecked")
      Map<String, InterpreterProperty> properties =
          (Map<String, InterpreterProperty>) manager.getByName("test").getProperties();
      JsonObject property1 = new Gson().toJsonTree(properties.get("property_1")).getAsJsonObject();
      assertFalse(property1.has("description"));
    } finally {
      manager.close();
    }
  }

  @Test
  void testSavedRepositoriesAreUsedAsIs() throws IOException {
    writeInterpreterJson(TEST_SETTING_ID);

    InterpreterSettingManager manager = newManager(true);
    try {
      // the file has only "central"; without the flag the runtime "local" repository is added
      assertEquals(1, manager.getRepositories().size());
      assertEquals("central", manager.getRepositories().get(0).getId());
    } finally {
      manager.close();
    }
  }

  @Test
  void testEmptyRepositoryListIsKept() throws IOException {
    writeInterpreterJson(TEST_SETTING_ID);
    File file = new File(confDir, "interpreter.json");
    JsonObject root = JsonParser.parseString(
        new String(readInterpreterJson(), StandardCharsets.UTF_8)).getAsJsonObject();
    root.add("interpreterRepositories", new JsonArray());
    Files.write(file.toPath(), root.toString().getBytes(StandardCharsets.UTF_8));

    InterpreterSettingManager manager = newManager(true);
    try {
      assertTrue(manager.getRepositories().isEmpty());
    } finally {
      manager.close();
    }
  }

  @Test
  void testFileIsNotRewrittenAtStartup() throws IOException {
    byte[] original = writeInterpreterJson(TEST_SETTING_ID);

    InterpreterSettingManager manager = newManager(true);
    try {
      assertArrayEquals(original, readInterpreterJson());
    } finally {
      manager.close();
    }
  }

  @Test
  void testFileIsSavedWhenSettingIsChanged() throws Exception {
    byte[] original = writeInterpreterJson(TEST_SETTING_ID);

    InterpreterSettingManager manager = newManager(true);
    try {
      InterpreterSetting setting = manager.getByName("test");
      @SuppressWarnings("unchecked")
      Map<String, InterpreterProperty> properties =
          (Map<String, InterpreterProperty>) setting.getProperties();
      properties.get("property_1").setValue("changed_value");
      manager.setPropertyAndRestart(setting.getId(), setting.getOption(), properties,
          new ArrayList<>());

      byte[] saved = readInterpreterJson();
      assertFalse(Arrays.equals(original, saved));
      // Zeppelin always uses the setting name as its id (InterpreterSetting.postProcessing),
      // so the legacy id of the test fixture is saved as "test"
      JsonObject savedSettings = JsonParser.parseString(new String(saved, StandardCharsets.UTF_8))
          .getAsJsonObject().getAsJsonObject("interpreterSettings");
      assertEquals(1, savedSettings.size());
      JsonObject savedSetting = savedSettings.getAsJsonObject("test");
      assertEquals("changed_value", savedSetting.getAsJsonObject("properties")
          .getAsJsonObject("property_1").get("value").getAsString());
      // still only the saved interpreter
      assertEquals(1, savedSetting.getAsJsonArray("interpreterGroup").size());
    } finally {
      manager.close();
    }
  }

  @Test
  void testFreshInstallStillCreatesSettings() throws IOException {
    Files.delete(new File(confDir, "interpreter.json").toPath());

    InterpreterSettingManager manager = newManager(true);
    try {
      assertFalse(manager.get().isEmpty());
      assertTrue(new File(confDir, "interpreter.json").exists());
    } finally {
      manager.close();
    }
  }
}
