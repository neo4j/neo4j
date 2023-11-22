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

import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.QuantifiedPath
import org.neo4j.cypher.internal.rewriting.conditions.noUnnamedNodesAndRelationships
import org.neo4j.cypher.internal.rewriting.rewriters.factories.ASTRewriterFactory
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo

/**
 * If node or relationship has an anonymous name, it is not yet included in the variableGroupings.
 * Since we generate predicates (e.g. relationship uniqueness) that live outside of the QPP and
 * need to see the group variable, we need to add those variables to the set of variableGroupings.
 */
case object AddQuantifiedPathAnonymousVariableGroupings extends StepSequencer.Step
    with StepSequencer.DefaultPostCondition
    with ASTRewriterFactory {

  val instance: Rewriter = bottomUp(
    Rewriter.lift {
      case qpp: QuantifiedPath =>
        QuantifiedPath(qpp.part, qpp.quantifier, qpp.optionalWhereExpression)(qpp.position)
    }
  )

  override def preConditions: Set[StepSequencer.Condition] = Set(
    noUnnamedNodesAndRelationships,
    normalizePredicates.completed
  )

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty

  override def getRewriter(
    semanticState: SemanticState,
    parameterTypeMapping: Map[String, ParameterTypeInfo],
    cypherExceptionFactory: CypherExceptionFactory,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator
  ): Rewriter = instance
}
