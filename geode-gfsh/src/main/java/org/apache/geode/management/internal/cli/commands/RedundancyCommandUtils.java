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

import static org.apache.geode.internal.cache.control.RestoreRedundancyResultsImpl.NO_REDUNDANT_COPIES_FOR_REGIONS;
import static org.apache.geode.internal.cache.control.RestoreRedundancyResultsImpl.PRIMARY_TRANSFERS_COMPLETED;
import static org.apache.geode.internal.cache.control.RestoreRedundancyResultsImpl.PRIMARY_TRANSFER_TIME;
import static org.apache.geode.internal.cache.control.RestoreRedundancyResultsImpl.REDUNDANCY_NOT_SATISFIED_FOR_REGIONS;
import static org.apache.geode.internal.cache.control.RestoreRedundancyResultsImpl.REDUNDANCY_SATISFIED_FOR_REGIONS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.geode.cache.control.RegionRedundancyStatus;
import org.apache.geode.cache.control.RestoreRedundancyResults;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.control.RestoreRedundancyResultsImpl;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.operation.RebalanceOperationPerformer;

public class RedundancyCommandUtils {
  public static final String NO_MEMBERS_WITH_VERSION_FOR_REGION =
      "No members with a version greater than or equal to %s were found for region %s";
  public static final String NO_MEMBERS_SECTION = "no-members";
  public static final String NO_MEMBERS_HEADER =
      "No partitioned regions were found.";
  public static final String NO_MEMBERS_FOR_REGION_SECTION = "no-members-for-region";
  public static final String NO_MEMBERS_FOR_REGION_HEADER =
      "No members hosting the following regions were found: ";
  public static final String ERROR_SECTION = "errors";
  public static final String ERROR_SECTION_HEADER =
      "The following errors or exceptions were encountered: ";
  public static final String SUMMARY_SECTION = "summary-section";
  public static final String ZERO_REDUNDANT_COPIES =
      "Number of regions with zero redundant copies = ";
  public static final String PARTIALLY_SATISFIED_REDUNDANCY =
      "Number of regions with partially satisfied redundancy = ";
  public static final String FULLY_SATISFIED_REDUNDANCY =
      "Number of regions with fully satisfied redundancy = ";
  public static final String ZERO_REDUNDANCY_SECTION = "zero-redundancy";
  public static final String UNDER_REDUNDANCY_SECTION = "under-redundancy";
  public static final String SATISFIED_REDUNDANCY_SECTION = "satisfied-redundancy";
  public static final String PRIMARIES_INFO_SECTION = "primaries-info";
  public static final String EXCEPTION_MEMBER_MESSAGE = "Exception occurred on member %s: %s";
  public static final Version REDUNDANCY_COMMAND_ADDED_VERSION = Version.GEODE_1_13_0;
  public static final String INDENT = "  ";

  void populateLists(List<RebalanceOperationPerformer.MemberPRInfo> membersForEachRegion,
      List<String> noMemberRegions, String[] includeRegions, String[] excludeRegions,
      InternalCache cache) {
    // Include all regions
    if (includeRegions == null) {
      // Exclude these regions
      List<String> excludedRegionList =
          excludeRegions != null ? Arrays.asList(excludeRegions) : new ArrayList<>();

      List<RebalanceOperationPerformer.MemberPRInfo> memberRegionList =
          getMembersForEachRegion(excludedRegionList, cache);
      membersForEachRegion.addAll(memberRegionList);
    } else {
      for (String regionName : includeRegions) {
        DistributedMember memberForRegion = getOneMemberForRegion(regionName, cache);

        // If we did not find a member for this region name, add it to the list of regions with no
        // members
        if (memberForRegion == null) {
          noMemberRegions.add(regionName);
        } else {
          RebalanceOperationPerformer.MemberPRInfo memberPRInfo =
              new RebalanceOperationPerformer.MemberPRInfo();
          memberPRInfo.region = regionName;
          memberPRInfo.dsMemberList.add(memberForRegion);
          membersForEachRegion.add(memberPRInfo);
        }
      }
    }
  }

  // Extracted for testing
  List<RebalanceOperationPerformer.MemberPRInfo> getMembersForEachRegion(
      List<String> excludedRegionList, InternalCache cache) {
    return RebalanceOperationPerformer.getMemberRegionList(
        ManagementService.getManagementService(cache), cache, excludedRegionList);
  }

  // Extracted for testing
  DistributedMember getOneMemberForRegion(String regionName, InternalCache cache) {
    String regionNameWithSeparator = regionName;
    // The getAssociatedMembers method requires region names start with '/'
    if (!regionName.startsWith("/")) {
      regionNameWithSeparator = "/" + regionName;
    }
    return RebalanceOperationPerformer.getAssociatedMembers(regionNameWithSeparator, cache);
  }

  List<DistributedMember> filterViableMembersForVersion(
      RebalanceOperationPerformer.MemberPRInfo memberPRInfo, Version version) {
    return memberPRInfo.dsMemberList.stream()
        .map(InternalDistributedMember.class::cast)
        .filter(member -> member.getVersionObject().compareTo(version) >= 0)
        .collect(Collectors.toList());
  }

  ResultModel getNoViableMembersResult(Version version, String regionName) {
    ResultModel result = new ResultModel();
    InfoResultModel errorSection = result.addInfo(ERROR_SECTION);
    errorSection.setHeader(ERROR_SECTION_HEADER);
    errorSection
        .addLine(String.format(NO_MEMBERS_WITH_VERSION_FOR_REGION, version.getName(), regionName));
    result.setStatus(Result.Status.ERROR);
    return result;
  }

  ResultModel buildResultModelFromFunctionResults(List<CliFunctionResult> functionResults,
      List<String> includedRegionsWithNoMembers, boolean isStatusCommand) {
    ResultModel result = new ResultModel();

    // No members hosting partitioned regions were found, but no regions were explicitly included,
    // so return OK status
    if (functionResults.size() == 0 && includedRegionsWithNoMembers.size() == 0) {
      return createNoMembersResultModel(result);
    }

    RestoreRedundancyResultsImpl resultCollector = getNewRestoreRedundancyResults();
    List<String> errorStrings = new ArrayList<>();

    for (CliFunctionResult functionResult : functionResults) {
      if (functionResult.getResultObject() == null) {
        errorStrings.add(String.format(EXCEPTION_MEMBER_MESSAGE, functionResult.getMemberIdOrName(),
            functionResult.getStatusMessage()));
      } else {
        RestoreRedundancyResults resultObject =
            (RestoreRedundancyResults) functionResult.getResultObject();
        resultCollector.addRegionResults(resultObject);
      }
    }

    // Exceptions were encountered while executing functions,
    if (errorStrings.size() != 0) {
      return createErrorResultModel(result, errorStrings);
    }

    // At least one explicitly included region was not found, so return error status along with the
    // results for the regions that were found
    if (includedRegionsWithNoMembers.size() > 0) {
      addRegionsWithNoMembersSection(includedRegionsWithNoMembers, result);
    }

    addSummarySection(result, resultCollector);
    addZeroRedundancySection(result, resultCollector);
    addUnderRedundancySection(result, resultCollector);
    addSatisfiedRedundancySection(result, resultCollector);

    // Status command output does not include info on reassigning primaries
    if (!isStatusCommand) {
      addPrimariesSection(result, resultCollector);

      // If redundancy was not fully restored, return error status
      if (resultCollector.getStatus().equals(RestoreRedundancyResults.Status.FAILURE)) {
        result.setStatus(Result.Status.ERROR);
      }
    }
    return result;
  }

  private ResultModel createNoMembersResultModel(ResultModel result) {
    InfoResultModel noMembersSection = result.addInfo(NO_MEMBERS_SECTION);
    noMembersSection.setHeader(NO_MEMBERS_HEADER);
    return result;
  }

  private ResultModel createErrorResultModel(ResultModel result, List<String> errorStrings) {
    InfoResultModel errorSection = result.addInfo(ERROR_SECTION);
    errorSection.setHeader(ERROR_SECTION_HEADER);
    errorStrings.forEach(errorSection::addLine);
    result.setStatus(Result.Status.ERROR);
    return result;
  }

  private void addRegionsWithNoMembersSection(List<String> regionsWithNoMembers,
      ResultModel result) {
    InfoResultModel noMembersSection = result.addInfo(NO_MEMBERS_FOR_REGION_SECTION);
    noMembersSection.setHeader(NO_MEMBERS_FOR_REGION_HEADER);
    regionsWithNoMembers.forEach(noMembersSection::addLine);
    result.setStatus(Result.Status.ERROR);
  }

  private void addSummarySection(ResultModel result, RestoreRedundancyResults resultCollector) {
    InfoResultModel summary = result.addInfo(SUMMARY_SECTION);
    int satisfied = resultCollector.getSatisfiedRedundancyRegionResults().size();
    int notSatisfied = resultCollector.getUnderRedundancyRegionResults().size();
    int zeroRedundancy = resultCollector.getZeroRedundancyRegionResults().size();
    summary.addLine(ZERO_REDUNDANT_COPIES + zeroRedundancy);
    summary.addLine(PARTIALLY_SATISFIED_REDUNDANCY + notSatisfied);
    summary.addLine(FULLY_SATISFIED_REDUNDANCY + satisfied);
  }

  private void addZeroRedundancySection(ResultModel result,
      RestoreRedundancyResults resultCollector) {
    Map<String, RegionRedundancyStatus> zeroRedundancyResults =
        resultCollector.getZeroRedundancyRegionResults();
    if (zeroRedundancyResults.size() > 0) {
      InfoResultModel zeroRedundancy = result.addInfo(ZERO_REDUNDANCY_SECTION);
      zeroRedundancy.setHeader(NO_REDUNDANT_COPIES_FOR_REGIONS);
      zeroRedundancyResults.values().stream().map(RegionRedundancyStatus::toString)
          .forEach(string -> zeroRedundancy.addLine(INDENT + string));
    }
  }

  private void addUnderRedundancySection(ResultModel result,
      RestoreRedundancyResults resultCollector) {
    Map<String, RegionRedundancyStatus> underRedundancyResults =
        resultCollector.getUnderRedundancyRegionResults();
    if (underRedundancyResults.size() > 0) {
      InfoResultModel underRedundancy = result.addInfo(UNDER_REDUNDANCY_SECTION);
      underRedundancy.setHeader(REDUNDANCY_NOT_SATISFIED_FOR_REGIONS);
      underRedundancyResults.values().stream().map(RegionRedundancyStatus::toString)
          .forEach(string -> underRedundancy.addLine(INDENT + string));
    }
  }

  private void addSatisfiedRedundancySection(ResultModel result,
      RestoreRedundancyResults resultCollector) {
    Map<String, RegionRedundancyStatus> satisfiedRedundancyResults =
        resultCollector.getSatisfiedRedundancyRegionResults();
    if (satisfiedRedundancyResults.size() > 0) {
      InfoResultModel satisfiedRedundancy = result.addInfo(SATISFIED_REDUNDANCY_SECTION);
      satisfiedRedundancy.setHeader(REDUNDANCY_SATISFIED_FOR_REGIONS);
      satisfiedRedundancyResults.values().stream().map(RegionRedundancyStatus::toString)
          .forEach(string -> satisfiedRedundancy.addLine(INDENT + string));
    }
  }

  private void addPrimariesSection(ResultModel result,
      RestoreRedundancyResults resultCollector) {
    InfoResultModel primaries = result.addInfo(PRIMARIES_INFO_SECTION);
    primaries
        .addLine(PRIMARY_TRANSFERS_COMPLETED + resultCollector.getTotalPrimaryTransfersCompleted());
    primaries
        .addLine(PRIMARY_TRANSFER_TIME + resultCollector.getTotalPrimaryTransferTime().toMillis());
  }

  // Extracted for testing
  RestoreRedundancyResultsImpl getNewRestoreRedundancyResults() {
    return new RestoreRedundancyResultsImpl();
  }
}
