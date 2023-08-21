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

import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.Or
import org.neo4j.cypher.internal.expressions.Xor
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.rewriting.AstRewritingMonitor
import org.neo4j.cypher.internal.rewriting.conditions.AndsAboveOrs
import org.neo4j.cypher.internal.rewriting.conditions.NoInequalityInsideNot
import org.neo4j.cypher.internal.rewriting.conditions.NoXorOperators
import org.neo4j.cypher.internal.rewriting.conditions.NotsBelowBooleanOperators
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.rewriting.rewriters.copyVariables
import org.neo4j.cypher.internal.rewriting.rewriters.repeatWithSizeLimit
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.bottomUp

case class deMorganRewriter()(implicit monitor: AstRewritingMonitor) extends Rewriter {

  def apply(that: AnyRef): AnyRef = instance(that)

  private val step = Rewriter.lift {
    case p @ Xor(expr1, expr2) =>
      And(
        Or(expr1, expr2)(p.position),
        Not(And(expr1.endoRewrite(copyVariables), expr2.endoRewrite(copyVariables))(p.position))(p.position)
      )(p.position)
    case p @ Not(And(exp1, exp2)) =>
      Or(Not(exp1)(p.position), Not(exp2)(p.position))(p.position)
    case p @ Not(Or(exp1, exp2)) =>
      And(Not(exp1)(p.position), Not(exp2)(p.position))(p.position)
  }

  private val instance: Rewriter = repeatWithSizeLimit(bottomUp(step))(monitor)
}

case object deMorganRewriter extends CnfPhase {

  override def instance(from: BaseState, context: BaseContext): Rewriter = {
    implicit val monitor: AstRewritingMonitor = context.monitors.newMonitor[AstRewritingMonitor]()
    deMorganRewriter()
  }

  override def preConditions: Set[StepSequencer.Condition] = Set.empty

  override def postConditions: Set[StepSequencer.Condition] = Set(NotsBelowBooleanOperators, NoXorOperators)

  override def invalidatedConditions: Set[StepSequencer.Condition] =
    SemanticInfoAvailable ++ Set(
      AndsAboveOrs,
      NoInequalityInsideNot
    )
}
