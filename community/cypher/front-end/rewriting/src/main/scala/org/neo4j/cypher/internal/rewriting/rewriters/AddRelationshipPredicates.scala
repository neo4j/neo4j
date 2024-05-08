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

import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.ParenthesizedPath
import org.neo4j.cypher.internal.expressions.PathPatternPart
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.PatternPartWithSelector
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.rewriting.conditions.containsNoNodesOfType
import org.neo4j.cypher.internal.rewriting.conditions.noUnnamedNodesAndRelationships
import org.neo4j.cypher.internal.rewriting.rewriters.factories.ASTRewriterFactory
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.DefaultPostCondition
import org.neo4j.cypher.internal.util.StepSequencer.Step
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo

trait AddRelationshipPredicates[NC] extends Step with DefaultPostCondition with ASTRewriterFactory {

  override def preConditions: Set[StepSequencer.Condition] = Set(
    noUnnamedNodesAndRelationships,
    AddQuantifiedPathAnonymousVariableGroupings.completed,
    // We cannot add such predicates in PatternExpression/PatternComprehension,
    // so they should have been rewritten at this point
    containsNoNodesOfType[PatternExpression](),
    containsNoNodesOfType[PatternComprehension]()
  )

  override def invalidatedConditions: Set[StepSequencer.Condition] = SemanticInfoAvailable

  val rewriter: Rewriter

  override def getRewriter(
    semanticState: SemanticState,
    parameterTypeMapping: Map[String, ParameterTypeInfo],
    cypherExceptionFactory: CypherExceptionFactory,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
    cancellationChecker: CancellationChecker
  ): Rewriter = rewriter

  protected def rewriteSelectivePatternPart(part: PatternPartWithSelector): PatternPartWithSelector =
    part.element match {
      case path: ParenthesizedPath =>
        val nodeConnections = collectNodeConnections(path.part.element)
        val predicate = createPredicateFor(nodeConnections, path.position)
        val whereExpr = path.optionalWhereClause
        val newPredicate = Where.combineOrCreateExpressionBeforeCnf(whereExpr, predicate)(whereExpr.map(_.position))
        val newElement = path.copy(optionalWhereClause = newPredicate)(path.position)
        part.replaceElement(newElement)
      case otherElement =>
        val nodeConnections = collectNodeConnections(otherElement)
        createPredicateFor(nodeConnections, part.position) match {
          // We should not wrap the pattern in new parentheses if there is no predicate to add
          case None => part
          case Some(predicate) =>
            val syntheticPatternPart = PathPatternPart(otherElement)
            val newElement = ParenthesizedPath(syntheticPatternPart, Some(predicate))(part.position)
            part.replaceElement(newElement)
        }
    }

  protected def withPredicates(pattern: ASTNode, nodeConnections: Seq[NC], where: Option[Where]): Option[Where] = {
    val pos = pattern.position
    val maybePredicate: Option[Expression] = createPredicateFor(nodeConnections, pos)
    Where.combineOrCreateBeforeCnf(where, maybePredicate)(pos)
  }

  protected def createPredicateFor(nodeConnections: Seq[NC], pos: InputPosition): Option[Expression] = {
    createPredicatesFor(nodeConnections, pos).reduceOption(expressions.And(_, _)(pos))
  }

  def createPredicatesFor(nodeConnections: Seq[NC], pos: InputPosition): Seq[Expression]

  def collectNodeConnections(pattern: ASTNode): Seq[NC]

  def createPredicatesFor(pattern: ASTNode): Seq[Expression] = {
    val connections = collectNodeConnections(pattern)
    createPredicatesFor(connections, pattern.position)
  }
}
