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
package org.apache.geode.distributed.internal.membership.gms.api;

import java.net.InetAddress;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.internal.MakeImmutable;
import org.apache.geode.distributed.internal.DistributionConfig;

public interface MembershipConfig {
  /** stall time to wait for concurrent join/leave/remove requests to be received */
  long MEMBER_REQUEST_COLLECTION_INTERVAL =
      Long.getLong(DistributionConfig.GEMFIRE_PREFIX + "member-request-collection-interval", 300);
  /** in a small cluster we might want to involve all members in operations */
  int SMALL_CLUSTER_SIZE = 9;

  @MakeImmutable
  int[] DEFAULT_MEMBERSHIP_PORT_RANGE = {41000, 61000};

  int DEFAULT_LOCATOR_WAIT_TIME = 0;
  String DEFAULT_SECURITY_UDP_DHALGO = "";
  int DEFAULT_UDP_FRAGMENT_SIZE = 60000;
  String DEFAULT_START_LOCATOR = "";
  int DEFAULT_MEMBER_TIMEOUT = 5000;
  int DEFAULT_LOSS_THRESHOLD = 51;
  int DEFAULT_MEMBER_WEIGHT = 0;
  boolean DEFAULT_ENABLE_NETWORK_PARTITION_DETECTION = true;
  int DEFAULT_MCAST_PORT = 0;
  String DEFAULT_LOCATORS = "";
  String DEFAULT_BIND_ADDRESS = "";
  String DEFAULT_SECURITY_PEER_AUTH_INIT = "";
  boolean DEFAULT_DISABLE_TCP = false;
  String DEFAULT_NAME = "";
  String DEFAULT_ROLES = "";
  String DEFAULT_GROUPS = "";
  String DEFAULT_DURABLE_CLIENT_ID = "";

  default boolean isReconnecting() { return false; }

  default int getLocatorWaitTime() { return DEFAULT_LOCATOR_WAIT_TIME; }

  long getJoinTimeout();

  default int[] getMembershipPortRange() { return DEFAULT_MEMBERSHIP_PORT_RANGE; }

  default long getMemberTimeout() {return DEFAULT_MEMBER_TIMEOUT; }

  default int getLossThreshold() { return DEFAULT_LOSS_THRESHOLD; }

  default int getMemberWeight() { return DEFAULT_MEMBER_WEIGHT; }

  default boolean isMulticastEnabled() { return getMcastPort() > 0; }

  default boolean isNetworkPartitionDetectionEnabled() { return DEFAULT_ENABLE_NETWORK_PARTITION_DETECTION; }

  default boolean isUDPSecurityEnabled() { return ! getSecurityUDPDHAlgo().isEmpty(); }

  default boolean areLocatorsPreferredAsCoordinators() { return isNetworkPartitionDetectionEnabled(); }

  default String getSecurityUDPDHAlgo() { return DEFAULT_SECURITY_UDP_DHALGO; }

  default int getMcastPort() { return DEFAULT_MCAST_PORT; }

  default String getLocators() { return DEFAULT_LOCATORS; }

  default String getStartLocator() { return DEFAULT_START_LOCATOR; }

  default String getBindAddress() { return DEFAULT_BIND_ADDRESS; };

  default String getSecurityPeerAuthInit() {return DEFAULT_SECURITY_PEER_AUTH_INIT; }

  default boolean getDisableTcp() { return DEFAULT_DISABLE_TCP; }

  default String getName() { return DEFAULT_NAME; }

  default String getRoles() { return DEFAULT_ROLES; }

  default String getGroups() { return DEFAULT_GROUPS; }

  default String getDurableClientId() { return DEFAULT_DURABLE_CLIENT_ID; }

  default int getDurableClientTimeout();

  default InetAddress getMcastAddress();

  default int getMcastTtl();

  default int getMcastSendBufferSize();

  default int getMcastRecvBufferSize();

  default int getUdpFragmentSize() { return DEFAULT_UDP_FRAGMENT_SIZE; }

  default int getUdpRecvBufferSize();

  default int getUdpSendBufferSize();

  default int getMcastByteAllowance();

  default float getMcastRechargeThreshold();

  default int getMcastRechargeBlockMs();

  default long getAckWaitThreshold();

  default boolean getDisableAutoReconnect();

  default int getSecurityPeerMembershipTimeout();

  default long getAckSevereAlertThreshold();

  default int getVmKind();

  default boolean isMcastEnabled();

  default boolean isTcpDisabled();

  default Object getOldDSMembershipInfo();

  default boolean getIsReconnectingDS();
}
