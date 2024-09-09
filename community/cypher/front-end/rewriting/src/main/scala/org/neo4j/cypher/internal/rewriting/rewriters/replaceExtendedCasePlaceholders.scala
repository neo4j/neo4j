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
package org.neo4j.cypher.internal.rewriting.rewriters

import org.neo4j.cypher.internal.expressions.CaseExpression
import org.neo4j.cypher.internal.expressions.CaseExpression.Placeholder
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.rewriting.rewriters.factories.PreparatoryRewritingRewriterFactory
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewritable.RewritableAny
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.DefaultPostCondition
import org.neo4j.cypher.internal.util.topDown

/**
 * When we parse a CASE statement, we insert placeholders into predicate expressions like so:
 *
 * CASE myFunc()
 * WHEN IS NULL THEN 1 // IsNull(Placeholder)
 * WHEN < 0.5 THEN 2   // LessThan(Placeholder, 0.5)
 *
 * This rewriter replaces the placeholders with variables, to be allocated as Expression Variables by the runtime.
 *
 * The reason this is not done directly in the parser is that we have to generate unique names for the variables,
 * and we do not have access to [[AnonymousVariableNameGenerator]] there. It could also impact caching.
 * */
object replaceExtendedCasePlaceholders extends StepSequencer.Step with DefaultPostCondition
    with PreparatoryRewritingRewriterFactory {

  def preConditions: Set[StepSequencer.Condition] = Set.empty
  def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty

  private def rewritePlaceholders(name: LogicalVariable): Rewriter =
    topDown(
      Rewriter.lift {
        case Placeholder => name
      },
      stopper = _.isInstanceOf[CaseExpression]
    )

  def getRewriter(cypherExceptionFactory: CypherExceptionFactory, nameGen: AnonymousVariableNameGenerator): Rewriter =
    topDown(Rewriter.lift {
      case c: CaseExpression if c.candidate.isDefined && c.candidateVarName.isEmpty =>
        val name = varFor(nameGen.nextName)

        c.copy(
          candidateVarName = Some(name),
          alternatives = c.alternatives.map { case (predicate, result) =>
            predicate.endoRewrite(rewritePlaceholders(name)) -> result
          }
        )(c.position)
    })
}
