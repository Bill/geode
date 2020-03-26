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
package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.cache.control.RestoreRedundancyResults.Status.FAILURE;
import static org.apache.geode.cache.control.RestoreRedundancyResults.Status.SUCCESS;
import static org.apache.geode.internal.cache.control.RestoreRedundancyResultsImpl.NO_REDUNDANT_COPIES_FOR_REGIONS;
import static org.apache.geode.internal.cache.control.RestoreRedundancyResultsImpl.PRIMARY_TRANSFERS_COMPLETED;
import static org.apache.geode.internal.cache.control.RestoreRedundancyResultsImpl.PRIMARY_TRANSFER_TIME;
import static org.apache.geode.internal.cache.control.RestoreRedundancyResultsImpl.REDUNDANCY_NOT_SATISFIED_FOR_REGIONS;
import static org.apache.geode.internal.cache.control.RestoreRedundancyResultsImpl.REDUNDANCY_SATISFIED_FOR_REGIONS;
import static org.apache.geode.management.cli.Result.Status.ERROR;
import static org.apache.geode.management.cli.Result.Status.OK;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.ERROR_SECTION;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.ERROR_SECTION_HEADER;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.EXCEPTION_MEMBER_MESSAGE;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.FULLY_SATISFIED_REDUNDANCY;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.INDENT;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.NO_MEMBERS_FOR_REGION_HEADER;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.NO_MEMBERS_FOR_REGION_SECTION;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.NO_MEMBERS_HEADER;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.NO_MEMBERS_SECTION;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.NO_MEMBERS_WITH_VERSION_FOR_REGION;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.PARTIALLY_SATISFIED_REDUNDANCY;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.PRIMARIES_INFO_SECTION;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.SATISFIED_REDUNDANCY_SECTION;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.SUMMARY_SECTION;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.UNDER_REDUNDANCY_SECTION;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.ZERO_REDUNDANCY_SECTION;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.ZERO_REDUNDANT_COPIES;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.control.RegionRedundancyStatus;
import org.apache.geode.cache.control.RestoreRedundancyResults;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.control.RestoreRedundancyResultsImpl;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.operation.RebalanceOperationPerformer;

public class RedundancyCommandUtilsTest {

  List<CliFunctionResult> functionResults;
  private RestoreRedundancyResultsImpl mockResultCollector;
  private RedundancyCommandUtils utils;
  public final int transfersCompleted = 5;
  public final long transferTime = 1234;

  @Before
  public void setUp() {
    mockResultCollector = mock(RestoreRedundancyResultsImpl.class);
    when(mockResultCollector.getStatus()).thenReturn(SUCCESS);
    when(mockResultCollector.getTotalPrimaryTransfersCompleted()).thenReturn(transfersCompleted);
    when(mockResultCollector.getTotalPrimaryTransferTime())
        .thenReturn(Duration.ofMillis(transferTime));

    utils = spy(new RedundancyCommandUtils());
    doReturn(mockResultCollector).when(utils).getNewRestoreRedundancyResults();

    CliFunctionResult successResult = mock(CliFunctionResult.class);
    RestoreRedundancyResults mockResultObject = mock(RestoreRedundancyResults.class);
    when(successResult.getResultObject()).thenReturn(mockResultObject);

    String regionName = "regionName";
    RegionRedundancyStatus mockRegionResult = mock(RegionRedundancyStatus.class);
    Map<String, RegionRedundancyStatus> regionResults =
        Collections.singletonMap(regionName, mockRegionResult);
    when(mockResultObject.getRegionResults()).thenReturn(regionResults);

    functionResults = Collections.singletonList(successResult);
  }

  @Test
  public void populateListsCorrectlyPopulatesListsWhenIncludeRegionsIsNull() {
    List<RebalanceOperationPerformer.MemberPRInfo> expectedListContents = new ArrayList<>();
    expectedListContents.add(new RebalanceOperationPerformer.MemberPRInfo());
    doReturn(expectedListContents).when(utils).getMembersForEachRegion(any(), any());

    String[] excludedRegions = {"region1", "region2"};
    List<String> expectedExcludedRegions = Arrays.asList(excludedRegions);

    List<RebalanceOperationPerformer.MemberPRInfo> membersForEachRegion = new ArrayList<>();
    List<String> noMemberRegions = new ArrayList<>();
    utils.populateLists(membersForEachRegion, noMemberRegions, null, excludedRegions, null);

    assertThat(membersForEachRegion, is(expectedListContents));
    assertThat(noMemberRegions.size(), is(0));

    // Confirm that the correct excluded regions were passed as arguments
    verify(utils, times(1)).getMembersForEachRegion(eq(expectedExcludedRegions), any());
  }

  @Test
  public void populateListsCorrectlyPopulatesListsWhenIncludeRegionsIsNotNull() {
    String noMemberRegionName = "noMemberRegion";
    List<String> expectedNoMemberRegions = Collections.singletonList(noMemberRegionName);
    String regionName = "testRegion";
    DistributedMember mockMember = mock(DistributedMember.class);

    doReturn(null).when(utils).getOneMemberForRegion(eq(noMemberRegionName), any());
    doReturn(mockMember).when(utils).getOneMemberForRegion(eq(regionName), any());

    String[] includedRegions = {noMemberRegionName, regionName};

    List<RebalanceOperationPerformer.MemberPRInfo> membersForEachRegion = new ArrayList<>();
    List<String> noMemberRegions = new ArrayList<>();
    utils.populateLists(membersForEachRegion, noMemberRegions, includedRegions, null, null);

    assertThat(noMemberRegions, is(expectedNoMemberRegions));
    assertThat(membersForEachRegion.size(), is(1));
    assertThat(membersForEachRegion.get(0).region, is(regionName));
    assertThat(membersForEachRegion.get(0).dsMemberList, hasItem(mockMember));
  }

  @Test
  public void filterViableMembersForVersionReturnsListWithOnlyMembersWithVersionGreaterThanOrEqualToSpecifiedVersion() {
    InternalDistributedMember oldVersionMember = mock(InternalDistributedMember.class);
    Version oldVersion = Version.GEODE_1_1_0;
    when(oldVersionMember.getVersionObject()).thenReturn(oldVersion);
    InternalDistributedMember matchingVersionMember = mock(InternalDistributedMember.class);
    Version matchingVersion = Version.GEODE_1_10_0;
    when(matchingVersionMember.getVersionObject()).thenReturn(matchingVersion);
    InternalDistributedMember newerVersionMember = mock(InternalDistributedMember.class);
    Version newerVersion = Version.GEODE_1_13_0;
    when(newerVersionMember.getVersionObject()).thenReturn(newerVersion);

    RebalanceOperationPerformer.MemberPRInfo memberPrInfo =
        new RebalanceOperationPerformer.MemberPRInfo();
    memberPrInfo.dsMemberList.add(oldVersionMember);
    memberPrInfo.dsMemberList.add(matchingVersionMember);
    memberPrInfo.dsMemberList.add(newerVersionMember);

    List<DistributedMember> filteredMembers = utils.filterViableMembersForVersion(memberPrInfo,
        matchingVersion);

    assertThat(filteredMembers, hasItems(matchingVersionMember, newerVersionMember));
    assertThat(filteredMembers, not(hasItem(oldVersionMember)));
  }

  @Test
  public void getNoViableMembersResultReturnsCorrectlyFormattedResultModel() {
    Version version = Version.CURRENT;
    String regionName = "testRegion";
    ResultModel result = utils.getNoViableMembersResult(version, regionName);

    assertThat(result.getStatus(), is(ERROR));
    InfoResultModel errorSection = result.getInfoSection(ERROR_SECTION);
    assertThat(errorSection.getHeader(), is(ERROR_SECTION_HEADER));
    assertThat(errorSection.getContent().size(), is(1));

    String expectedMessage =
        String.format(NO_MEMBERS_WITH_VERSION_FOR_REGION, version.getName(), regionName);
    assertThat(errorSection.getContent(), hasItem(expectedMessage));
  }

  @Test
  public void buildResultModelFromFunctionResultsReturnsErrorWhenFunctionResultListIsEmpty() {
    List<CliFunctionResult> emptyResultList = new ArrayList<>();
    ResultModel result =
        utils.buildResultModelFromFunctionResults(emptyResultList, new ArrayList<>(), false);

    assertThat(result.getStatus(), is(OK));
    assertThat(result.getInfoSection(NO_MEMBERS_SECTION).getHeader(),
        is(NO_MEMBERS_HEADER));
  }

  @Test
  public void buildResultModelFromFunctionResultsReturnsErrorWhenFunctionResultsHaveNoResultObject() {
    CliFunctionResult errorResult1 = mock(CliFunctionResult.class);
    String errorMessage1 = "error1";
    String memberName1 = "member1";
    when(errorResult1.getStatusMessage()).thenReturn(errorMessage1);
    when(errorResult1.getMemberIdOrName()).thenReturn(memberName1);

    CliFunctionResult errorResult2 = mock(CliFunctionResult.class);
    String errorMessage2 = "error2";
    String memberName2 = "member2";
    when(errorResult2.getStatusMessage()).thenReturn(errorMessage2);
    when(errorResult2.getMemberIdOrName()).thenReturn(memberName2);

    List<CliFunctionResult> errorResults = new ArrayList<>();
    errorResults.add(errorResult1);
    errorResults.add(errorResult2);

    String expectedMessage1 = String.format(EXCEPTION_MEMBER_MESSAGE, memberName1, errorMessage1);
    String expectedMessage2 = String.format(EXCEPTION_MEMBER_MESSAGE, memberName2, errorMessage2);

    ResultModel result =
        utils.buildResultModelFromFunctionResults(errorResults, new ArrayList<>(), false);

    assertThat(result.getStatus(), is(ERROR));
    InfoResultModel errorSection = result.getInfoSection(ERROR_SECTION);
    assertThat(errorSection.getHeader(), is(ERROR_SECTION_HEADER));
    assertThat(errorSection.getContent().size(), is(2));
    assertThat(errorSection.getContent(), hasItems(expectedMessage1, expectedMessage2));
  }

  @Test
  public void buildResultModelFromFunctionResultsPopulatesResultCollectorWhenFunctionResultHasResultObject() {
    CliFunctionResult functionResult = mock(CliFunctionResult.class);
    RestoreRedundancyResults mockResultObject = mock(RestoreRedundancyResults.class);
    when(functionResult.getResultObject()).thenReturn(mockResultObject);

    utils.buildResultModelFromFunctionResults(Collections.singletonList(functionResult),
        new ArrayList<>(), false);

    verify(mockResultCollector, times(1)).addRegionResults(mockResultObject);
  }

  @Test
  public void buildResultModelFromFunctionResultsIncludesRegionsWithNoMembersSectionWhenSomeRegionsHaveNoMembers() {
    List<String> regionsWithNoMembers = new ArrayList<>();

    String noMembersRegion1 = "region1";
    String noMembersRegion2 = "region2";
    regionsWithNoMembers.add(noMembersRegion1);
    regionsWithNoMembers.add(noMembersRegion2);

    ResultModel result =
        utils.buildResultModelFromFunctionResults(functionResults, regionsWithNoMembers, false);

    InfoResultModel noMembersSection = result.getInfoSection(NO_MEMBERS_FOR_REGION_SECTION);
    assertThat(noMembersSection.getHeader(), is(NO_MEMBERS_FOR_REGION_HEADER));
    assertThat(noMembersSection.getContent(), hasItems(noMembersRegion1, noMembersRegion2));
  }

  @Test
  public void buildResultModelFromFunctionResultsReturnsErrorWhenIsStatusCommandIsFalseAndResultCollectorStatusIsFailure() {
    when(mockResultCollector.getStatus()).thenReturn(FAILURE);

    ResultModel result =
        utils.buildResultModelFromFunctionResults(functionResults, new ArrayList<>(), false);

    assertThat(result.getStatus(), is(ERROR));
  }

  @Test
  public void buildResultModelFromFunctionResultsReturnsOkWhenIsStatusCommandIsFalseAndResultCollectorStatusIsSuccess() {
    ResultModel result =
        utils.buildResultModelFromFunctionResults(functionResults, new ArrayList<>(), false);

    assertThat(result.getStatus(), is(OK));
  }

  @Test
  public void buildResultModelFromFunctionResultsReturnsOkWhenIsStatusCommandIsTrueAndResultCollectorStatusIsFailure() {
    when(mockResultCollector.getStatus()).thenReturn(FAILURE);

    ResultModel result =
        utils.buildResultModelFromFunctionResults(functionResults, new ArrayList<>(), true);

    assertThat(result.getStatus(), is(OK));
  }

  @Test
  public void buildResultModelFromFunctionResultsReturnsInfoSectionsForEachRegionResultStatusAndPrimaryInfoWhenIsStatusCommandIsFalse() {
    RegionRedundancyStatus zeroRedundancy = mock(RegionRedundancyStatus.class);
    Map<String, RegionRedundancyStatus> zeroRedundancyResults =
        Collections.singletonMap("zeroRedundancy", zeroRedundancy);
    when(mockResultCollector.getZeroRedundancyRegionResults()).thenReturn(zeroRedundancyResults);

    RegionRedundancyStatus underRedundancy = mock(RegionRedundancyStatus.class);
    Map<String, RegionRedundancyStatus> underRedundancyResults =
        Collections.singletonMap("underRedundancy", underRedundancy);
    when(mockResultCollector.getUnderRedundancyRegionResults()).thenReturn(underRedundancyResults);

    RegionRedundancyStatus satisfiedRedundancy = mock(RegionRedundancyStatus.class);
    Map<String, RegionRedundancyStatus> satisfiedRedundancyResults =
        Collections.singletonMap("satisfiedRedundancy", satisfiedRedundancy);
    when(mockResultCollector.getSatisfiedRedundancyRegionResults())
        .thenReturn(satisfiedRedundancyResults);

    ResultModel result =
        utils.buildResultModelFromFunctionResults(functionResults, new ArrayList<>(), false);

    InfoResultModel summarySection = result.getInfoSection(SUMMARY_SECTION);
    assertThat(summarySection.getContent(), hasItem(ZERO_REDUNDANT_COPIES + 1));
    assertThat(summarySection.getContent(), hasItem(PARTIALLY_SATISFIED_REDUNDANCY + 1));
    assertThat(summarySection.getContent(), hasItem(FULLY_SATISFIED_REDUNDANCY + 1));

    InfoResultModel zeroRedundancySection = result.getInfoSection(ZERO_REDUNDANCY_SECTION);
    assertThat(zeroRedundancySection.getHeader(), is(NO_REDUNDANT_COPIES_FOR_REGIONS));
    assertThat(zeroRedundancySection.getContent(), hasItem(INDENT + zeroRedundancy.toString()));

    InfoResultModel underRedundancySection = result.getInfoSection(UNDER_REDUNDANCY_SECTION);
    assertThat(underRedundancySection.getHeader(), is(REDUNDANCY_NOT_SATISFIED_FOR_REGIONS));
    assertThat(underRedundancySection.getContent(), hasItem(INDENT + underRedundancy.toString()));

    InfoResultModel satisfiedRedundancySection =
        result.getInfoSection(SATISFIED_REDUNDANCY_SECTION);
    assertThat(satisfiedRedundancySection.getHeader(), is(REDUNDANCY_SATISFIED_FOR_REGIONS));
    assertThat(satisfiedRedundancySection.getContent(),
        hasItem(INDENT + satisfiedRedundancy.toString()));

    InfoResultModel primariesSection = result.getInfoSection(PRIMARIES_INFO_SECTION);
    assertThat(primariesSection.getContent(),
        hasItem(PRIMARY_TRANSFERS_COMPLETED + transfersCompleted));
    assertThat(primariesSection.getContent(), hasItem(PRIMARY_TRANSFER_TIME + transferTime));
  }

  @Test
  public void buildResultModelFromFunctionResultsDoesNotReturnPrimaryInfoWhenIsStatusCommandIsTrue() {
    ResultModel result =
        utils.buildResultModelFromFunctionResults(functionResults, new ArrayList<>(), true);

    assertThat(result.getInfoSection(PRIMARIES_INFO_SECTION), nullValue());
  }
}
