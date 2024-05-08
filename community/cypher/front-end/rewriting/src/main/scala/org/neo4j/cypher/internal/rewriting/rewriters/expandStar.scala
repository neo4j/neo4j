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

import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItem
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.ast.Yield
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.rewriting.conditions.containsNoReturnAll
import org.neo4j.cypher.internal.rewriting.rewriters.factories.ASTRewriterFactory
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.Condition
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo
import org.neo4j.cypher.internal.util.topDown

case object ProjectionClausesHaveSemanticInfo extends Condition

case class expandStar(state: SemanticState) extends Rewriter {

  override def apply(that: AnyRef): AnyRef = instance(that)

  private val rewriter = Rewriter.lift {
    case clause @ With(_, values, _, _, _, _, _) if values.includeExisting =>
      val newReturnItems =
        if (values.includeExisting) returnItems(clause, values.items, values.defaultOrderOnColumns) else values
      clause.copy(returnItems = newReturnItems)(clause.position)

    case clause @ Return(_, values, _, _, _, excludedNames, _) if values.includeExisting =>
      val newReturnItems =
        if (values.includeExisting) returnItems(clause, values.items, values.defaultOrderOnColumns, excludedNames)
        else values
      clause.copy(returnItems = newReturnItems, excludedNames = Set.empty)(clause.position)

    case clause @ Yield(values, _, _, _, _) if values.includeExisting =>
      val newReturnItems =
        if (values.includeExisting) returnItems(clause, values.items, values.defaultOrderOnColumns) else values
      clause.copy(returnItems = newReturnItems)(clause.position)

    case expandedAstNode =>
      expandedAstNode
  }

  private val instance = topDown(rewriter)

  private def returnItems(
    clause: Clause,
    listedItems: Seq[ReturnItem],
    defaultOrderOnColumns: Option[List[String]],
    excludedNames: Set[String] = Set.empty
  ): ReturnItems = {
    val scope = state.scope(clause).getOrElse {
      throw new IllegalStateException(s"${clause.name} should note its Scope in the SemanticState")
    }

    val clausePos = clause.position
    val symbolNames = scope.symbolNames -- excludedNames -- listedItems.map(returnItem => returnItem.name)
    val orderedSymbolNames = defaultOrderOnColumns.map(columns => {
      val newColumns = symbolNames -- columns
      val ordered = columns.filter(symbolNames.contains) ++ newColumns
      ordered.toIndexedSeq
    }).getOrElse(symbolNames.toIndexedSeq.sorted)
    val expandedItems = orderedSymbolNames.map { id =>
      // We use the position of the clause for variables in new return items.
      // If the position was one of previous declaration, that could destroy scoping.
      val expr = Variable(id)(clausePos)
      val alias = expr.copyId
      AliasedReturnItem(expr, alias)(clausePos)
    }

    val newItems = expandedItems ++ listedItems
    ReturnItems(includeExisting = false, newItems)(clausePos)
  }
}

case object expandStar extends StepSequencer.Step with ASTRewriterFactory {

  override def preConditions: Set[StepSequencer.Condition] = Set(
    ProjectionClausesHaveSemanticInfo // Looks up recorded scopes of projection clauses.
  )

  override def postConditions: Set[StepSequencer.Condition] = Set(containsNoReturnAll)

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set(
    ProjectionClausesHaveSemanticInfo
  )

  override def getRewriter(
    semanticState: SemanticState,
    parameterTypeMapping: Map[String, ParameterTypeInfo],
    cypherExceptionFactory: CypherExceptionFactory,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
    cancellationChecker: CancellationChecker
  ): Rewriter = expandStar(semanticState)
}
