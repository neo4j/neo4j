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
package org.neo4j.cypher.internal.rewriting

import org.neo4j.cypher.internal.rewriting.rewriters.ProjectionClausesHaveSemanticInfo
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.Condition

package object conditions {
  case object CallInvocationsResolved extends Condition
  case object GQLAliasFunctionNameRewritten extends Condition
  case object FunctionInvocationsResolved extends Condition
  case object PatternExpressionsHaveSemanticInfo extends Condition
  case object SizeOfCollectRewrittenToCount extends Condition
  case object SubqueryExpressionsHaveDependenciesInWithClauses extends Condition
  case object SensitiveLiteralsExtracted extends StepSequencer.Condition
  case object LiteralsExtracted extends StepSequencer.Condition
  case object PredicatesSimplified extends StepSequencer.Condition
  case object NotsBelowBooleanOperators extends StepSequencer.Condition
  case object NoXorOperators extends StepSequencer.Condition
  case object AndsAboveOrs extends StepSequencer.Condition
  case object AndRewrittenToAnds extends StepSequencer.Condition
  case object OrRewrittenToOrs extends StepSequencer.Condition
  case object NoInequalityInsideNot extends StepSequencer.Condition

  /**
   * A collection of all conditions that require semantic info available for some AST nodes
   */
  val SemanticInfoAvailable: Set[StepSequencer.Condition] = Set(
    PatternExpressionsHaveSemanticInfo,
    ProjectionClausesHaveSemanticInfo
  )
}
