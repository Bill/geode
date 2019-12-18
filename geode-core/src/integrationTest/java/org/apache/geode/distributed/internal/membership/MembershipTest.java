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

import static org.apache.geode.distributed.internal.membership.adapter.SocketCreatorAdapter.asTcpSocketCreator;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Answers;

import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.membership.adapter.SocketCreatorAdapter;
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
import org.apache.geode.distributed.internal.tcpserver.ConnectionWatcher;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.distributed.internal.tcpserver.TcpSocketCreator;
import org.apache.geode.internal.admin.SSLConfig;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.serialization.DSFIDSerializer;
import org.apache.geode.internal.serialization.DSFIDSerializerFactory;
import org.apache.geode.internal.serialization.DSFIDSerializerImpl;
import org.apache.geode.internal.serialization.ObjectDeserializer;
import org.apache.geode.internal.serialization.ObjectSerializer;

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

    final MembershipConfig config = new MembershipConfig() {
      public String getLocators() {
        return localHost.getHostName() + '[' + locator.getPort() + ']';
      }
    };

    DSFIDSerializer dsfidSerializer = new DSFIDSerializerFactory().create();

    final TcpSocketCreator socketCreator = asTcpSocketCreator(new SocketCreator(new SSLConfig.Builder().build()));

    TcpClient client = new TcpClient(socketCreator, dsfidSerializer.getObjectSerializer(), dsfidSerializer.getObjectDeserializer());

    @SuppressWarnings("unchecked")
    final Membership<InternalDistributedMember>
        membership =
        MembershipBuilder.<InternalDistributedMember>newMembershipBuilder()
            .setConfig(config)
            .setSerializer(dsfidSerializer)
            .setMemberIDFactory(new MemberIdentifierFactoryImpl())
            .setLocatorClient(client)
            .create();

    membership.start();
    assertThat(membership.getView().getMembers()).hasSize(1);
//    membership.startEventProcessing();
  }
}
