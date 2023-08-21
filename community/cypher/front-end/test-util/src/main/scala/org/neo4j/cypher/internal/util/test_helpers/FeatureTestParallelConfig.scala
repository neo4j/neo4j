/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.util.test_helpers

import org.junit.platform.engine.ConfigurationParameters
import org.junit.platform.engine.support.hierarchical.ParallelExecutionConfiguration
import org.junit.platform.engine.support.hierarchical.ParallelExecutionConfigurationStrategy

/**
 * Custom parallel execution configuration for feature tests.
 */
class FeatureTestParallelConfig extends ParallelExecutionConfigurationStrategy {

  override def createConfiguration(
    configurationParameters: ConfigurationParameters
  ): ParallelExecutionConfiguration = {
    new ParallelExecutionConfiguration {
      private val parallelism = Runtime.getRuntime.availableProcessors()
      override def getParallelism: Int = parallelism
      override def getMinimumRunnable: Int = 0
      override def getMaxPoolSize: Int = parallelism + 256
      override def getCorePoolSize: Int = parallelism
      override def getKeepAliveSeconds: Int = 30
    }
  }
}
