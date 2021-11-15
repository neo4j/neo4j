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
package org.neo4j.cypher.internal.ir

import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsParameters
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.helpers.ExpressionConverters
import org.neo4j.cypher.internal.ir.helpers.ExpressionConverters.PredicateConverter
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.exceptions.InternalException

trait QueryHorizon {

  def exposedSymbols(coveredIds: Set[String]): Set[String]

  def dependingExpressions: Seq[Expression]

  def dependencies: Set[String] = dependingExpressions.treeFold(Set.empty[String]) {
    case id: Variable =>
      acc => TraverseChildren(acc + id.name)
  }

  def readOnly = true

  /**
   * If dependingExpressions is empty, or only contains variables, we can assume that it doesn't contain any reads
   * @return 'true' if the this horizon might do database reads. 'false' otherwise.
   */
  def couldContainRead: Boolean = dependingExpressions.exists(!_.isInstanceOf[Variable])

    /**
     * @return all recursively included query graphs, with leaf information for Eagerness analysis.
     *         Query graphs from pattern expressions and pattern comprehensions will generate variable names that might clash with existing names, so this method
     *         is not safe to use for planning pattern expressions and pattern comprehensions.
     */
  protected def getAllQGsWithLeafInfo: Seq[QgWithLeafInfo] = {
    val filtered = dependingExpressions.filter(!_.isInstanceOf[Variable])
    val patternComprehensions = filtered.findAllByClass[PatternComprehension].map((e: PatternComprehension) => ExpressionConverters.asQueryGraph(e, e.dependencies.map(_.name), new AnonymousVariableNameGenerator))
    val patternExpressions = filtered.findAllByClass[PatternExpression].map((e: PatternExpression) => ExpressionConverters.asQueryGraph(e, e.dependencies.map(_.name), new AnonymousVariableNameGenerator))
    val qgs = patternComprehensions ++ patternExpressions :+ getQueryGraphFromDependingExpressions
    qgs.map(QgWithLeafInfo.qgWithNoStableIdentifierAndOnlyLeaves)
  }

  protected def getQueryGraphFromDependingExpressions: QueryGraph = {
    val dependencies = dependingExpressions
      .flatMap(_.dependencies)
      .map(_.name)
      .toSet

    QueryGraph(
      argumentIds = dependencies,
      selections = Selections.from(dependingExpressions),
    )
  }

  lazy val allQueryGraphs: Seq[QgWithLeafInfo] = getAllQGsWithLeafInfo
}

final case class PassthroughAllHorizon() extends QueryHorizon {
  override def exposedSymbols(coveredIds: Set[String]): Set[String] = coveredIds

  override def dependingExpressions: Seq[Expression] = Seq.empty

  override lazy val allQueryGraphs: Seq[QgWithLeafInfo] = Seq.empty
}

case class UnwindProjection(variable: String, exp: Expression) extends QueryHorizon {
  override def exposedSymbols(coveredIds: Set[String]): Set[String] = coveredIds + variable

  override def dependingExpressions: Seq[Expression] = Seq(exp)
}

case class LoadCSVProjection(variable: String, url: Expression, format: CSVFormat, fieldTerminator: Option[StringLiteral]) extends QueryHorizon {
  override def exposedSymbols(coveredIds: Set[String]): Set[String] = coveredIds + variable

  override def dependingExpressions: Seq[Expression] = Seq(url)
}

case class CallSubqueryHorizon(callSubquery: PlannerQueryPart,
                               correlated: Boolean,
                               yielding: Boolean,
                               inTransactionsParameters: Option[InTransactionsParameters]) extends QueryHorizon {
  override def exposedSymbols(coveredIds: Set[String]): Set[String] = coveredIds ++ callSubquery.returns

  override def dependingExpressions: Seq[Expression] = Seq.empty

  override def readOnly: Boolean = callSubquery.readOnly

  /**
   * We don't analyze the subquery but just assume that it's doing reads.
   */
  override def couldContainRead: Boolean = true

  override lazy val allQueryGraphs: Seq[QgWithLeafInfo] = super.getAllQGsWithLeafInfo ++ callSubquery.allQGsWithLeafInfo
}

sealed abstract class QueryProjection extends QueryHorizon {
  def selections: Selections
  def projections: Map[String, Expression]
  def queryPagination: QueryPagination
  def keySet: Set[String]
  def withSelection(selections: Selections): QueryProjection
  def withAddedProjections(projections: Map[String, Expression]): QueryProjection
  def withPagination(queryPagination: QueryPagination): QueryProjection

  override def dependingExpressions: Seq[Expression] = projections.values.toSeq ++ selections.predicates.map(_.expr)

  def updatePagination(f: QueryPagination => QueryPagination): QueryProjection = withPagination(f(queryPagination))

  def addPredicates(predicates: Expression*): QueryProjection = {
    val newSelections = Selections(predicates.flatMap(_.asPredicates).toSet)
    withSelection(selections = selections ++ newSelections)
  }
}

object QueryProjection {
  val empty = RegularQueryProjection()

  def forIds(coveredIds: Set[String]) =
    coveredIds.toIndexedSeq.map(idName =>
      AliasedReturnItem(Variable(idName)(null), Variable(idName)(null))(null, false))

  def combine(lhs: QueryProjection, rhs: QueryProjection): QueryProjection = (lhs, rhs) match {
    case (left: RegularQueryProjection, right: RegularQueryProjection) =>
      left ++ right

    case _ =>
      throw new InternalException("Aggregations cannot be combined")
  }
}

final case class RegularQueryProjection(projections: Map[String, Expression] = Map.empty,
                                        queryPagination: QueryPagination = QueryPagination.empty,
                                        selections: Selections = Selections()) extends QueryProjection {
  def keySet: Set[String] = projections.keySet

  def ++(other: RegularQueryProjection) =
    RegularQueryProjection(
      projections = projections ++ other.projections,
      queryPagination = queryPagination ++ other.queryPagination,
      selections = selections ++ other.selections
    )

  override def withAddedProjections(projections: Map[String, Expression]): RegularQueryProjection =
    copy(projections = this.projections ++ projections)

  def withPagination(queryPagination: QueryPagination): RegularQueryProjection =
    copy(queryPagination = queryPagination)

  override def exposedSymbols(coveredIds: Set[String]): Set[String] = projections.keySet

  override def withSelection(selections: Selections): QueryProjection = copy(selections = selections)
}

final case class AggregatingQueryProjection(groupingExpressions: Map[String, Expression] = Map.empty,
                                            aggregationExpressions: Map[String, Expression] = Map.empty,
                                            queryPagination: QueryPagination = QueryPagination.empty,
                                            selections: Selections = Selections()) extends QueryProjection {

  assert(
    !(groupingExpressions.isEmpty && aggregationExpressions.isEmpty),
    "Everything can't be empty"
  )

  override def projections: Map[String, Expression] = groupingExpressions ++ aggregationExpressions

  override def keySet: Set[String] = groupingExpressions.keySet ++ aggregationExpressions.keySet

  override def dependingExpressions: Seq[Expression] = super.dependingExpressions ++ aggregationExpressions.values

  override def withAddedProjections(groupingKeys: Map[String, Expression]): AggregatingQueryProjection =
    copy(groupingExpressions = this.groupingExpressions ++ groupingKeys)

  override def withPagination(queryPagination: QueryPagination): AggregatingQueryProjection =
    copy(queryPagination = queryPagination)

  override def exposedSymbols(coveredIds: Set[String]): Set[String] = groupingExpressions.keySet ++ aggregationExpressions.keySet

  override def withSelection(selections: Selections): QueryProjection = copy(selections = selections)
}

final case class DistinctQueryProjection(groupingExpressions: Map[String, Expression] = Map.empty,
                                         queryPagination: QueryPagination = QueryPagination.empty,
                                         selections: Selections = Selections()) extends QueryProjection {

  def projections: Map[String, Expression] = groupingExpressions

  def keySet: Set[String] = groupingExpressions.keySet

  override def withAddedProjections(groupingKeys: Map[String, Expression]): DistinctQueryProjection =
    copy(groupingExpressions = this.groupingExpressions ++ groupingKeys)

  override def withPagination(queryPagination: QueryPagination): DistinctQueryProjection =
    copy(queryPagination = queryPagination)

  override def exposedSymbols(coveredIds: Set[String]): Set[String] = groupingExpressions.keySet

  override def withSelection(selections: Selections): QueryProjection = copy(selections = selections)
}
