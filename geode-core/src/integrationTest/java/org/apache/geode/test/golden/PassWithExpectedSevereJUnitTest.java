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
package org.apache.geode.test.golden;


import org.apache.geode.internal.logging.LocalLogWriter;
import org.apache.geode.internal.logging.LogWriterImpl;

/**
 * Verifies that an example test should always pass even if the output contains a severe that is
 * expected.
 *
 */
public class PassWithExpectedSevereJUnitTest extends PassWithExpectedProblemTestCase {

  @Override
  String problem() {
    return "severe";
  }

  @Override
  void outputProblemInProcess(final String message) {
    new LocalLogWriter(LogWriterImpl.INFO_LEVEL).severe(message);
  }

  public static void main(final String[] args) throws Exception {
    new PassWithExpectedSevereJUnitTest().executeInProcess();
  }
}
