/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.management.internal.cli;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category(DistributedTest.class)
public class NetstatDUnitTest {
  @ClassRule
  public static ClusterStartupRule lsRule = new ClusterStartupRule();

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private static MemberVM locator0, server0, server1;

  private static final String GROUP_1 = "group-1";

  private static final String GROUP_2 = "group-2";

  private static String netStatLsofCommand;

  @BeforeClass
  public static void beforeClass() throws Exception {
    locator0 = lsRule.startLocatorVM(0);

    Properties props = new Properties();
    props.setProperty("groups", GROUP_1);
    server0 = lsRule.startServerVM(1, props, locator0.getPort());
    props.setProperty("groups", GROUP_2);
    server1 = lsRule.startServerVM(2, props, locator0.getPort());

    gfsh.connectAndVerify(locator0);

    netStatLsofCommand = "netstat --with-lsof=true --member=" + server1.getName();

  }

  @Test
  public void testOutputToConsoleForAllMembers() throws Exception {
    CommandResult result = gfsh.executeCommand("netstat");
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);

    List<String> lines = new ArrayList<>();
    while (result.hasNextLine()) {
      lines.add(result.nextLine());
    }

    assertThat(lines.size()).isGreaterThan(5);
    assertThat(lines.get(4).trim().split("[,\\s]+")).containsExactlyInAnyOrder("locator-0",
        "server-1", "server-2");
  }

  @Test
  public void testOutputToConsoleForOneMember() throws Exception {
    CommandResult result = gfsh.executeCommand("netstat --member=server-1");
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);

    List<String> lines = new ArrayList<>();
    while (result.hasNextLine()) {
      lines.add(result.nextLine());
    }

    assertThat(lines.size()).isGreaterThan(5);
    assertThat(lines.get(4).trim().split("[,\\s]+")).containsExactlyInAnyOrder("server-1");
  }

  @Test
  public void testOutputToConsoleWithLsofForOneMember() throws Exception {
    CommandResult result = gfsh.executeCommand("netstat --member=server-1 --with-lsof");
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);

    List<String> lines = new ArrayList<>();
    while (result.hasNextLine()) {
      lines.add(result.nextLine());
    }

    assertThat(lines.size()).isGreaterThan(5);
    assertThat(lines.get(4).trim().split("[,\\s]+")).containsExactlyInAnyOrder("server-1");
    assertThat(lines).filteredOn(e -> e.contains("## lsof output ##")).hasSize(1);
  }

  @Test
  public void testOutputToFile() throws Exception {
    File outputFile = new File(temp.newFolder(), "command.log.txt");

    CommandResult result = gfsh.executeCommand("netstat --file=" + outputFile.getAbsolutePath());
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);

    List<String> lines = new ArrayList<>();
    Scanner scanner = new Scanner(outputFile);
    while (scanner.hasNextLine()) {
      lines.add(scanner.nextLine());
    }

    assertThat(lines.size()).isGreaterThan(5);
    assertThat(lines.get(4).trim().split("[,\\s]+")).containsExactlyInAnyOrder("locator-0",
        "server-1", "server-2");
  }

  @Test
  public void testOutputToFileForOneGroup() throws Exception {
    File outputFile = new File(temp.newFolder(), "command.log.txt");

    CommandResult result = gfsh.executeCommand(
        String.format("netstat --file=%s --group=%s", outputFile.getAbsolutePath(), GROUP_1));
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);

    List<String> lines = new ArrayList<>();
    Scanner scanner = new Scanner(outputFile);
    while (scanner.hasNextLine()) {
      lines.add(scanner.nextLine());
    }

    assertThat(lines.size()).isGreaterThan(5);
    assertThat(lines.get(4).trim().split("[,\\s]+")).containsExactly("server-1");
  }

  @Test
  public void testOutputWithLsofToFile() throws Exception {
    File outputFile = new File(temp.newFolder(), "command.log.txt");

    CommandResult result =
        gfsh.executeCommand("netstat --with-lsof=true --file=" + outputFile.getAbsolutePath());
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);

    List<String> lines = new ArrayList<>();
    Scanner scanner = new Scanner(outputFile);
    while (scanner.hasNextLine()) {
      lines.add(scanner.nextLine());
    }

    assertThat(lines.size()).isGreaterThan(5);
    assertThat(lines.get(4).trim().split("[,\\s]+")).containsExactlyInAnyOrder("locator-0",
        "server-1", "server-2");
    assertThat(lines).filteredOn(e -> e.contains("## lsof output ##")).hasSize(1);
  }

  @Ignore("GEODE-2488")
  @Test
  public void testConnectToLocatorWithLargeCommandResponse() throws Exception {
    gfsh.connect(server0.getEmbeddedLocatorPort(), GfshCommandRule.PortType.locator);
    gfsh.executeAndAssertThat(netStatLsofCommand).statusIsSuccess();
  }

  @Ignore("GEODE-2488")
  @Test
  public void testConnectToJmxManagerOneWithLargeCommandResponse() throws Exception {
    gfsh.connect(server0.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    gfsh.executeAndAssertThat(netStatLsofCommand).statusIsSuccess();
  }

  @Ignore("GEODE-2488")
  @Test
  public void testConnectToJmxManagerTwoWithLargeCommandResponse() throws Exception {
    gfsh.connect(server1.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    gfsh.executeAndAssertThat(netStatLsofCommand).statusIsSuccess();
  }
}