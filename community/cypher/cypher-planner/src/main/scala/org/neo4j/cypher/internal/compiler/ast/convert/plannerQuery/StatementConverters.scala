/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.Create
import org.neo4j.cypher.internal.ast.ProjectingUnionAll
import org.neo4j.cypher.internal.ast.ProjectingUnionDistinct
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.QueryPart
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Start
import org.neo4j.cypher.internal.ast.UnaliasedReturnItem
import org.neo4j.cypher.internal.ast.UnionAll
import org.neo4j.cypher.internal.ast.UnionDistinct
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery.ClauseConverters.addToLogicalPlanInput
import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.Or
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternElement
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.PatternPart
import org.neo4j.cypher.internal.ir.PeriodicCommit
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.ir.PlannerQueryPart
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.UnionQuery
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.exceptions.InternalException

import scala.collection.mutable.ArrayBuffer

object StatementConverters {

  /**
   * Convert an AST SingleQuery into an IR SinglePlannerQuery
   */
  private def toPlannerQuery(
    q: SingleQuery,
    semanticTable: SemanticTable,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
    cancellationChecker: CancellationChecker,
    nonTerminating: Boolean
  ): SinglePlannerQuery = {
    val importedVariables: Set[String] = q.importWith.map((wth: With) =>
      wth.returnItems.items.map(_.name).toSet
    ).getOrElse(Set.empty)

    val builder = PlannerQueryBuilder(semanticTable, importedVariables)
    addClausesToPlannerQueryBuilder(q.clauses, builder, anonymousVariableNameGenerator, cancellationChecker, nonTerminating).build()
  }

  /**
   * Add all given clauses to a PlannerQueryBuilder and return the updated builder.
   */
  def addClausesToPlannerQueryBuilder(
    clauses: Seq[Clause],
    builder: PlannerQueryBuilder,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
    cancellationChecker: CancellationChecker,
    nonTerminating: Boolean
  ): PlannerQueryBuilder = {
    val flattenedClauses = flattenCreates(clauses)
    val slidingClauses = (flattenedClauses :+ null).sliding(2)
    slidingClauses.foldLeft(builder) {
      case (acc, Seq(clause, nextClause)) if nextClause != null =>
        cancellationChecker.throwIfCancelled()
        addToLogicalPlanInput(acc, clause, Some(nextClause), anonymousVariableNameGenerator, cancellationChecker, nonTerminating)
      case (acc, Seq(clause, _*)) =>
        cancellationChecker.throwIfCancelled()
        addToLogicalPlanInput(acc, clause, None, anonymousVariableNameGenerator, cancellationChecker, nonTerminating)
    }
  }

  private val NODE_BLACKLIST: Set[Class[_ <: ASTNode]] = Set(
    classOf[And],
    classOf[Or],
    classOf[UnaliasedReturnItem],
    classOf[Start],
    classOf[UnionAll],
    classOf[UnionDistinct]
  )


  private def findBlacklistedNodes(queryPart: QueryPart): Seq[ASTNode] = {
    queryPart.folder.treeFold(Seq.empty[ASTNode]) {
      case node: ASTNode if NODE_BLACKLIST.contains(node.getClass) =>
        acc => TraverseChildren(acc :+ node)
    }
  }

  def toPlannerQuery(
    query: Query,
    semanticTable: SemanticTable,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
    cancellationChecker: CancellationChecker,
    nonTerminating: Boolean
  ): PlannerQuery = {
    val plannerQueryPart = toPlannerQueryPart(query.part, semanticTable, anonymousVariableNameGenerator, cancellationChecker)
    PlannerQuery(plannerQueryPart, PeriodicCommit(query.periodicCommitHint))
  }

  def toPlannerQueryPart(
    queryPart: QueryPart,
    semanticTable: SemanticTable,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
    cancellationChecker: CancellationChecker,
    nonTerminating: Boolean = false
  ): PlannerQueryPart = {
    val nodes = findBlacklistedNodes(queryPart)
    require(nodes.isEmpty, "Found a blacklisted AST node: " + nodes.head.toString)

    queryPart match {
      case singleQuery: SingleQuery =>
        toPlannerQuery(singleQuery, semanticTable, anonymousVariableNameGenerator, cancellationChecker, nonTerminating)

      case unionQuery: ast.ProjectingUnion =>
        val part: PlannerQueryPart = toPlannerQueryPart(unionQuery.part, semanticTable, anonymousVariableNameGenerator, cancellationChecker, nonTerminating)
        val query: SinglePlannerQuery = toPlannerQuery(unionQuery.query, semanticTable, anonymousVariableNameGenerator, cancellationChecker, nonTerminating)

        val distinct = unionQuery match {
          case _: ProjectingUnionAll => false
          case _: ProjectingUnionDistinct => true
        }

        UnionQuery(part, query, distinct, unionQuery.unionMappings)
      case _ =>
        throw new InternalException(s"Received an AST-clause that has no representation the QG: $queryPart")
    }
  }

  /**
   * Flatten consecutive CREATE clauses into one.
   *
   *   CREATE (a) CREATE (b) => CREATE (a),(b)
   */
  private def flattenCreates(clauses: Seq[Clause]): Seq[Clause] = {
    val builder = ArrayBuffer.empty[Clause]
    var prevCreate: Option[(Seq[PatternPart], InputPosition)] = None
    for (clause <- clauses) {
      (clause, prevCreate) match {
        case (c: Create, None) if containsPatternExpression(c) =>
          builder += c
          prevCreate = None

        case (c: Create, None) =>
          prevCreate = Some((c.pattern.patternParts, c.position))

        case (c: Create, Some((prevParts, pos))) if !containsPatternExpression(c) =>
          prevCreate = Some((prevParts ++ c.pattern.patternParts, pos))

        case (nonMixingClause, Some((prevParts, pos))) =>
          builder += Create(Pattern(prevParts)(pos))(pos)
          builder += nonMixingClause
          prevCreate = None

        case (nonCreate, None) =>
          builder += nonCreate
      }
    }
    for ((prevParts, pos) <- prevCreate)
      builder += Create(Pattern(prevParts)(pos))(pos)
    builder
  }

  private def containsPatternExpression(c: Create): Boolean =
    c.pattern.patternParts.exists(part => containsPatternExpression(part.element))

  private def containsPatternExpression(element: PatternElement): Boolean =
    element.folder.treeExists {
      case _: PatternComprehension | _: PatternExpression => true
    }
}
