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
package org.apache.geode.distributed.internal.membership;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Answers;

import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.membership.gms.MemberIdentifierFactoryImpl;
import org.apache.geode.distributed.internal.membership.gms.api.Authenticator;
import org.apache.geode.distributed.internal.membership.gms.api.LifecycleListener;
import org.apache.geode.distributed.internal.membership.gms.api.MemberIdentifier;
import org.apache.geode.distributed.internal.membership.gms.api.Membership;
import org.apache.geode.distributed.internal.membership.gms.api.MembershipBuilder;
import org.apache.geode.distributed.internal.membership.gms.api.MembershipConfig;
import org.apache.geode.distributed.internal.membership.gms.api.MembershipListener;
import org.apache.geode.distributed.internal.membership.gms.api.MembershipStatistics;
import org.apache.geode.distributed.internal.membership.gms.api.MessageListener;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.internal.serialization.DSFIDSerializer;

public class MembershipTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private InternalLocator locator;
  private InetAddress localHost;

  @Before
  public void before() throws IOException {
    localHost = InetAddress.getLocalHost();

    locator = InternalLocator.startLocator(0, new File(""), null, null, localHost, false,
        new Properties(), null, temporaryFolder.getRoot().toPath());
  }

  @After
  public void after() {
    locator.stop();
  }

  @Test
  public void locatorStarts() {
    assertThat(locator.getPort()).isGreaterThan(0);
  }

  @Test
  public void memberCanConnectToLocator() {

    final MembershipConfig config =
        mock(MembershipConfig.class, withSettings().defaultAnswer(Answers.CALLS_REAL_METHODS));

    when(config.getLocators()).thenReturn(localHost.getHostName() + '[' + locator.getPort() + ']');

    final Membership<MemberIdentifier>
        membership =
        MembershipBuilder.newMembershipBuilder()
            .setAuthenticator(mock(Authenticator.class))
            .setStatistics(mock(MembershipStatistics.class))
            .setMembershipListener(mock(MembershipListener.class))
            .setMessageListener(mock(MessageListener.class))
            .setConfig(config)
            .setSerializer(mock(DSFIDSerializer.class))
            .setMemberIDFactory(new MemberIdentifierFactoryImpl())
            .setLifecycleListener(mock(LifecycleListener.class))
            .setLocatorClient(mock(TcpClient.class))
            .create();

    membership.start();
    assertThat(membership.getView().getMembers()).hasSize(1);
//    membership.startEventProcessing();
  }
}
