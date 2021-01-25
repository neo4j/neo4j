/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.rewriting

import org.neo4j.cypher.internal.rewriting.rewriters.ProjectionClausesHaveSemanticInfo
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.Condition

package object conditions {
  case object PatternExpressionsHaveSemanticInfo extends Condition
  case object PatternExpressionAreWrappedInExists extends Condition
  // This means that there is a SemanticTable, not that it is up to date
  case object StateContainsSemanticTable extends Condition

  /**
   * A collection of all conditions that require semantic info available for some AST nodes
   */
  val SemanticInfoAvailable: Set[StepSequencer.Condition] = Set(
    PatternExpressionsHaveSemanticInfo,
    ProjectionClausesHaveSemanticInfo
  )
}
