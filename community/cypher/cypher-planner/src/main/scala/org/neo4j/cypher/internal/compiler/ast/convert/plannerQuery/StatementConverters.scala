/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.Create
import org.neo4j.cypher.internal.ast.CreateOrInsert
import org.neo4j.cypher.internal.ast.ProjectingUnionAll
import org.neo4j.cypher.internal.ast.ProjectingUnionDistinct
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.UnaliasedReturnItem
import org.neo4j.cypher.internal.ast.UnionAll
import org.neo4j.cypher.internal.ast.UnionDistinct
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery.ClauseConverters.addToLogicalPlanInput
import org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery.composite.CompositeQueryConverter
import org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery.composite.CompositeQueryFragmenter
import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NonPrefixedPatternPart
import org.neo4j.cypher.internal.expressions.Or
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternElement
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.ir.QueryProjection
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.UnionQuery
import org.neo4j.cypher.internal.ir.ast.IRExpression
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.exceptions.InternalException

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

object StatementConverters {

  /**
   * Convert an AST SingleQuery into an IR SinglePlannerQuery
   */
  private def toSinglePlannerQuery(
    q: SingleQuery,
    semanticTable: SemanticTable,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
    cancellationChecker: CancellationChecker,
    importedVariables: Set[LogicalVariable],
    position: QueryProjection.Position
  ): SinglePlannerQuery = {
    val allImportedVars = importedVariables ++ q.importWith.map((wth: With) =>
      wth.returnItems.items.map(_.asInstanceOf[AliasedReturnItem].variable).toSet
    ).getOrElse(Set.empty)

    val builder = PlannerQueryBuilder(semanticTable, allImportedVars)
    addClausesToPlannerQueryBuilder(
      q.clauses,
      builder,
      anonymousVariableNameGenerator,
      cancellationChecker,
      position
    ).build()
  }

  /**
   * Add all given clauses to a PlannerQueryBuilder and return the updated builder.
   */
  def addClausesToPlannerQueryBuilder(
    clauses: Seq[Clause],
    builder: PlannerQueryBuilder,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
    cancellationChecker: CancellationChecker,
    position: QueryProjection.Position
  ): PlannerQueryBuilder = {
    @tailrec
    def addClausesToPlannerQueryBuilderRec(clauses: Seq[Clause], builder: PlannerQueryBuilder): PlannerQueryBuilder =
      if (clauses.isEmpty)
        builder
      else {
        cancellationChecker.throwIfCancelled()
        val clause = clauses.head
        val nextClauses = clauses.tail
        val nextClause = nextClauses.headOption
        val newBuilder =
          addToLogicalPlanInput(
            builder,
            clause,
            nextClause,
            anonymousVariableNameGenerator,
            cancellationChecker,
            position
          )
        addClausesToPlannerQueryBuilderRec(nextClauses, newBuilder)
      }

    addClausesToPlannerQueryBuilderRec(flattenCreates(clauses), builder)
  }

  private val NODE_BLACKLIST: Set[Class[_ <: ASTNode]] = Set(
    classOf[And],
    classOf[Or],
    classOf[UnaliasedReturnItem],
    classOf[UnionAll],
    classOf[UnionDistinct]
  )

  private def findBlacklistedNodes(query: Query): Seq[ASTNode] = {
    query.folder.treeFold(Seq.empty[ASTNode]) {
      case node: ASTNode if NODE_BLACKLIST.contains(node.getClass) =>
        acc => TraverseChildren(acc :+ node)
    }
  }

  private def rewriteAndCheckQuery(
    query: Query,
    semanticTable: SemanticTable,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator
  ): Query = {
    val rewrittenQuery = query.endoRewrite(CreateIrExpressions(anonymousVariableNameGenerator, semanticTable))
    val nodes = findBlacklistedNodes(query)
    require(nodes.isEmpty, "Found a blacklisted AST node: " + nodes.head.toString)
    rewrittenQuery
  }

  /**
   * Converts an AST query into a planner query.
   * It first rewrites sub-query expressions to IR expressions and traverses the whole AST to ensure the absence of blacklisted nodes.
   * If the AST has already been rewritten and checked, like in the case of the nested member of a union or a sub-query, consider calling [[convertToNestedPlannerQuery]] instead.
   */
  def convertToPlannerQuery(
    query: Query,
    semanticTable: SemanticTable,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
    cancellationChecker: CancellationChecker,
    importedVariables: Set[LogicalVariable] = Set.empty,
    position: QueryProjection.Position = QueryProjection.Position.Final
  ): PlannerQuery = {
    val rewrittenQuery = rewriteAndCheckQuery(query, semanticTable, anonymousVariableNameGenerator)
    convertToNestedPlannerQuery(
      rewrittenQuery,
      semanticTable,
      anonymousVariableNameGenerator,
      cancellationChecker,
      importedVariables,
      position
    )
  }

  /**
   * Converts an AST query into a planner query.
   * This function assumes that sub-query expressions have already been rewritten, see [[convertToPlannerQuery]].
   */
  def convertToNestedPlannerQuery(
    query: Query,
    semanticTable: SemanticTable,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
    cancellationChecker: CancellationChecker,
    importedVariables: Set[LogicalVariable] = Set.empty,
    position: QueryProjection.Position = QueryProjection.Position.Final
  ): PlannerQuery = {
    query match {
      case singleQuery: SingleQuery =>
        toSinglePlannerQuery(
          singleQuery,
          semanticTable,
          anonymousVariableNameGenerator,
          cancellationChecker,
          importedVariables,
          position
        )

      case unionQuery: ast.ProjectingUnion =>
        val lhs: PlannerQuery =
          convertToNestedPlannerQuery(
            unionQuery.lhs,
            semanticTable,
            anonymousVariableNameGenerator,
            cancellationChecker,
            importedVariables,
            QueryProjection.Position.Intermediate
          )
        val rhs: SinglePlannerQuery =
          toSinglePlannerQuery(
            unionQuery.rhs,
            semanticTable,
            anonymousVariableNameGenerator,
            cancellationChecker,
            importedVariables,
            QueryProjection.Position.Intermediate
          )

        val distinct = unionQuery match {
          case _: ProjectingUnionAll      => false
          case _: ProjectingUnionDistinct => true
        }

        UnionQuery(lhs, rhs, distinct, unionQuery.unionMappings)
      case _ =>
        throw new InternalException(s"Received an AST-clause that has no representation the QG: $query")
    }
  }

  /**
   * Converts an AST query into a planner query, in the context of a composite database, packaging up queries starting with USE to be executed on the targeted graph.
   * First rewrites sub-query expressions to IR expressions and traverses the whole AST to ensure the absence of blacklisted nodes.
   */
  def convertCompositePlannerQuery(
    query: Query,
    semanticTable: SemanticTable,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
    cancellationChecker: CancellationChecker
  ): PlannerQuery = {
    val rewrittenQuery = rewriteAndCheckQuery(query, semanticTable, anonymousVariableNameGenerator)
    val compositeQuery =
      CompositeQueryFragmenter.fragment(cancellationChecker, anonymousVariableNameGenerator, rewrittenQuery)
    CompositeQueryConverter.convert(cancellationChecker, anonymousVariableNameGenerator, semanticTable, compositeQuery)
  }

  /**
   * Flatten consecutive CREATE and INSERT clauses into one CREATE clause.
   *
   *   CREATE (a) CREATE (b) => CREATE (a),(b)
   *   INSERT (a) INSERT (b) => CREATE (a),(b)
   */
  private def flattenCreates(clauses: Seq[Clause]): Seq[Clause] = {
    val builder = ArrayBuffer.empty[Clause]
    var prevCreate: Option[(Seq[NonPrefixedPatternPart], InputPosition)] = None
    for (clause <- clauses) {
      (clause, prevCreate) match {
        case (c: CreateOrInsert, None) if containsIrExpression(c) =>
          builder += c
          prevCreate = None

        case (c: CreateOrInsert, None) =>
          prevCreate = Some((c.pattern.patternParts, c.position))

        case (c: CreateOrInsert, Some((prevParts, pos))) if !containsIrExpression(c) =>
          prevCreate = Some((prevParts ++ c.pattern.patternParts, pos))

        case (nonMixingClause, Some((prevParts, pos))) =>
          builder += Create(Pattern.ForUpdate(prevParts)(pos))(pos)
          builder += nonMixingClause
          prevCreate = None

        case (nonCreate, None) =>
          builder += nonCreate
      }
    }
    for ((prevParts, pos) <- prevCreate)
      builder += Create(Pattern.ForUpdate(prevParts)(pos))(pos)
    builder
  }.toSeq

  private def containsIrExpression(c: CreateOrInsert): Boolean =
    c.pattern.patternParts.exists(part => containsIrExpression(part.element))

  private def containsIrExpression(element: PatternElement): Boolean =
    element.folder.treeFindByClass[IRExpression].isDefined
}
