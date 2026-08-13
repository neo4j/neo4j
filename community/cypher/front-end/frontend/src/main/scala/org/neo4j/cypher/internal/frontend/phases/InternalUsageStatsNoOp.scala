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
package org.neo4j.cypher.internal.frontend.phases

case object InternalUsageStatsNoOp extends InternalUsageStats {
  override def incrementSyntaxUsageCount(key: SyntaxUsageMetricKey): Unit = ()

  override def getSyntaxUsageCount(key: SyntaxUsageMetricKey): Long = 0

  override def incrementSchemaInferenceUsageCount(key: SchemaInferenceUsageMetricKey): Unit = ()

  override def getSchemaInferenceUsageCount(key: SchemaInferenceUsageMetricKey): Long = 0

  override def incrementLeafPlanOperatorCount(key: LeafPlanOperatorMetricKey, count: Long): Unit = ()

  override def getLeafPlanOperatorCount(key: LeafPlanOperatorMetricKey): Long = 0
}
