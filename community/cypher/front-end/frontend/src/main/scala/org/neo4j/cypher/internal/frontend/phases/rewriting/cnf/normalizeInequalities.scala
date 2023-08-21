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
package org.neo4j.cypher.internal.frontend.phases.rewriting.cnf

import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.GreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.LessThan
import org.neo4j.cypher.internal.expressions.LessThanOrEqual
import org.neo4j.cypher.internal.expressions.Or
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.rewriting.conditions.AndsAboveOrs
import org.neo4j.cypher.internal.rewriting.conditions.OrRewrittenToOrs
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.topDown

case object InequalitiesNormalized extends StepSequencer.Condition

case object normalizeInequalities extends Rewriter with CnfPhase {

  override def apply(that: AnyRef): AnyRef = instance(that)

  private val instance: Rewriter = topDown(Rewriter.lift {
    case o @ Or(Equals(a1, b1), LessThan(a2, b2)) if a1 == a2 && b1 == b2 =>
      LessThanOrEqual(a1, b1)(o.position)
    case o @ Or(Equals(b1, a1), LessThan(a2, b2)) if a1 == a2 && b1 == b2 =>
      LessThanOrEqual(a1, b1)(o.position)
    case o @ Or(LessThan(a2, b2), Equals(a1, b1)) if a1 == a2 && b1 == b2 =>
      LessThanOrEqual(a1, b1)(o.position)
    case o @ Or(LessThan(a2, b2), Equals(b1, a1)) if a1 == a2 && b1 == b2 =>
      LessThanOrEqual(a1, b1)(o.position)
    case o @ Or(Equals(a1, b1), GreaterThan(a2, b2)) if a1 == a2 && b1 == b2 =>
      GreaterThanOrEqual(a1, b1)(o.position)
    case o @ Or(Equals(b1, a1), GreaterThan(a2, b2)) if a1 == a2 && b1 == b2 =>
      GreaterThanOrEqual(a1, b1)(o.position)
    case o @ Or(GreaterThan(a2, b2), Equals(a1, b1)) if a1 == a2 && b1 == b2 =>
      GreaterThanOrEqual(a1, b1)(o.position)
    case o @ Or(GreaterThan(a2, b2), Equals(b1, a1)) if a1 == a2 && b1 == b2 =>
      GreaterThanOrEqual(a1, b1)(o.position)
  })

  override def toString: String = "normalizeInequalities"

  override def instance(from: BaseState, context: BaseContext): Rewriter = this

  override def preConditions: Set[StepSequencer.Condition] = Set(
    // This rewriter matches on Or, so it must run before that is rewritten to Ors
    !OrRewrittenToOrs,
    // If the predicates are already in CNF (AndsAboveOrs), there are more opportunities for this rewriter to be applied
    AndsAboveOrs
  )

  override def postConditions: Set[StepSequencer.Condition] = Set(InequalitiesNormalized)

  override def invalidatedConditions: Set[StepSequencer.Condition] = SemanticInfoAvailable
}
