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
package org.neo4j.cypher.internal.ir

import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.CommandClause
import org.neo4j.cypher.internal.ast.Hint
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsParameters
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.ast.IRExpression
import org.neo4j.cypher.internal.ir.helpers.ExpressionConverters.PredicateConverter
import org.neo4j.cypher.internal.util.Foldable
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.exceptions.InternalException

sealed trait QueryHorizon extends Foldable {

  def exposedSymbols(coveredIds: Set[LogicalVariable]): Set[LogicalVariable]

  def dependingExpressions: Iterable[Expression]

  def dependencies: Set[LogicalVariable] = dependingExpressions.folder.findAllByClass[LogicalVariable].toSet

  def readOnly = true

  def allHints: Set[Hint]
  def withoutHints(hintsToIgnore: Set[Hint]): QueryHorizon
  def isTerminatingProjection: Boolean

  /**
   * If dependingExpressions is empty, or only contains variables, we can assume that it doesn't contain any reads
   * @return 'true' if this horizon might do database reads. 'false' otherwise.
   */
  def couldContainRead: Boolean = dependingExpressions.exists(!_.isInstanceOf[Variable]) || returnsNodesOrRelationships

  private def returnsNodesOrRelationships: Boolean = {
    this match {
      case qp: QueryProjection => qp.isTerminatingProjection && qp.projections.values.exists(_.isInstanceOf[Variable])
      case _                   => false
    }
  }

  /**
   * @return all recursively included query graphs, with leaf information for Eagerness analysis.
   *         Query graphs from pattern expressions and pattern comprehensions will generate variable names that might clash with existing names, so this method
   *         is not safe to use for planning pattern expressions and pattern comprehensions.
   */
  protected def getAllQGsWithLeafInfo: Seq[QgWithLeafInfo] = {
    val filtered = dependingExpressions.filter(!_.isInstanceOf[Variable]).toSeq
    val iRExpressions: Seq[QgWithLeafInfo] = filtered.folder.findAllByClass[IRExpression].flatMap((e: IRExpression) =>
      e.query.allQGsWithLeafInfo
    )
    QgWithLeafInfo.qgWithNoStableIdentifierAndOnlyLeaves(
      getQueryGraphFromDependingExpressions,
      this.isTerminatingProjection
    ) +: iRExpressions
  }

  protected def getQueryGraphFromDependingExpressions: QueryGraph = {
    val dependencies = dependingExpressions
      .flatMap(_.dependencies)
      .map(_.name)
      .toSet

    QueryGraph(
      argumentIds = dependencies,
      selections = Selections.from(dependingExpressions)
    )
  }

  lazy val allQueryGraphs: Seq[QgWithLeafInfo] = getAllQGsWithLeafInfo
}

final case class PassthroughAllHorizon() extends QueryHorizon {
  override def exposedSymbols(coveredIds: Set[LogicalVariable]): Set[LogicalVariable] = coveredIds

  override def dependingExpressions: Seq[Expression] = Seq.empty

  override lazy val allQueryGraphs: Seq[QgWithLeafInfo] = Seq.empty

  override def allHints: Set[Hint] = Set.empty

  override def withoutHints(hintsToIgnore: Set[Hint]): QueryHorizon = this

  override def isTerminatingProjection: Boolean = false
}

case class UnwindProjection(variable: LogicalVariable, exp: Expression) extends QueryHorizon {
  override def exposedSymbols(coveredIds: Set[LogicalVariable]): Set[LogicalVariable] = coveredIds + variable

  override def dependingExpressions: Seq[Expression] = Seq(exp)

  override def allHints: Set[Hint] = Set.empty

  override def withoutHints(hintsToIgnore: Set[Hint]): QueryHorizon = this

  override def isTerminatingProjection: Boolean = false
}

case class LoadCSVProjection(
  variable: LogicalVariable,
  url: Expression,
  format: CSVFormat,
  fieldTerminator: Option[StringLiteral]
) extends QueryHorizon {
  override def exposedSymbols(coveredIds: Set[LogicalVariable]): Set[LogicalVariable] = coveredIds + variable

  override def dependingExpressions: Seq[Expression] = Seq(url)

  override def allHints: Set[Hint] = Set.empty

  override def withoutHints(hintsToIgnore: Set[Hint]): QueryHorizon = this

  override def isTerminatingProjection: Boolean = false
}

case class CallSubqueryHorizon(
  callSubquery: PlannerQuery,
  correlated: Boolean,
  yielding: Boolean,
  inTransactionsParameters: Option[InTransactionsParameters]
) extends QueryHorizon {

  override def exposedSymbols(coveredIds: Set[LogicalVariable]): Set[LogicalVariable] = {
    val maybeReportAs = inTransactionsParameters.flatMap(_.reportParams.map(_.reportAs))
    coveredIds ++ callSubquery.returns.map(varFor) ++ maybeReportAs.toSeq
  }

  override def dependingExpressions: Seq[Expression] = Seq.empty

  override def readOnly: Boolean = callSubquery.readOnly

  override def allHints: Set[Hint] = callSubquery.allHints

  override def withoutHints(hintsToIgnore: Set[Hint]): QueryHorizon =
    copy(callSubquery = callSubquery.withoutHints(hintsToIgnore))

  /**
   * We don't analyze the subquery but just assume that it's doing reads.
   */
  override def couldContainRead: Boolean = true

  override lazy val allQueryGraphs: Seq[QgWithLeafInfo] = super.getAllQGsWithLeafInfo ++ callSubquery.allQGsWithLeafInfo

  override def isTerminatingProjection: Boolean = false
}

sealed abstract class QueryProjection extends QueryHorizon {
  def selections: Selections
  def projections: Map[LogicalVariable, Expression]
  def queryPagination: QueryPagination
  def keySet: Set[LogicalVariable]
  def isTerminating: Boolean
  override def isTerminatingProjection: Boolean = isTerminating
  def withSelection(selections: Selections): QueryProjection
  def withAddedProjections(projections: Map[LogicalVariable, Expression]): QueryProjection
  def withPagination(queryPagination: QueryPagination): QueryProjection
  def withIsTerminating(boolean: Boolean): QueryProjection

  override def dependingExpressions: Iterable[Expression] = projections.view.values ++ selections.predicates.map(_.expr)

  def updatePagination(f: QueryPagination => QueryPagination): QueryProjection = withPagination(f(queryPagination))

  def addPredicates(predicates: Expression*): QueryProjection = {
    val newSelections = Selections(predicates.flatMap(_.asPredicates).toSet)
    withSelection(selections = selections ++ newSelections)
  }

  override def allHints: Set[Hint] = Set.empty

  override def withoutHints(hintsToIgnore: Set[Hint]): QueryHorizon = this
}

object QueryProjection {
  def empty: RegularQueryProjection = RegularQueryProjection()

  def forIds(coveredIds: Set[LogicalVariable]): Seq[AliasedReturnItem] =
    coveredIds.toIndexedSeq.map(idName =>
      AliasedReturnItem(idName, idName)(InputPosition.NONE)
    )

  def combine(lhs: QueryProjection, rhs: QueryProjection): QueryProjection = (lhs, rhs) match {
    case (left: RegularQueryProjection, right: RegularQueryProjection) =>
      left ++ right

    case _ =>
      throw new InternalException("Aggregations cannot be combined")
  }
}

final case class RegularQueryProjection(
  projections: Map[LogicalVariable, Expression] = Map.empty,
  queryPagination: QueryPagination = QueryPagination.empty,
  selections: Selections = Selections(),
  isTerminating: Boolean = false
) extends QueryProjection {
  def keySet: Set[LogicalVariable] = projections.keySet

  def ++(other: RegularQueryProjection): RegularQueryProjection =
    RegularQueryProjection(
      projections = projections ++ other.projections,
      queryPagination = queryPagination ++ other.queryPagination,
      selections = selections ++ other.selections,
      isTerminating = isTerminating && other.isTerminating
    )

  override def withIsTerminating(boolean: Boolean): RegularQueryProjection =
    copy(isTerminating = boolean)

  override def withAddedProjections(projections: Map[LogicalVariable, Expression]): RegularQueryProjection =
    copy(projections = this.projections ++ projections)

  def withPagination(queryPagination: QueryPagination): RegularQueryProjection =
    copy(queryPagination = queryPagination)

  override def exposedSymbols(coveredIds: Set[LogicalVariable]): Set[LogicalVariable] = projections.keySet

  override def withSelection(selections: Selections): QueryProjection = copy(selections = selections)
}

final case class AggregatingQueryProjection(
  groupingExpressions: Map[LogicalVariable, Expression] = Map.empty,
  aggregationExpressions: Map[LogicalVariable, Expression] = Map.empty,
  queryPagination: QueryPagination = QueryPagination.empty,
  selections: Selections = Selections(),
  isTerminating: Boolean = false
) extends QueryProjection {

  assert(
    !(groupingExpressions.isEmpty && aggregationExpressions.isEmpty),
    "Everything can't be empty"
  )

  override def withIsTerminating(boolean: Boolean): AggregatingQueryProjection =
    copy(isTerminating = boolean)

  override def projections: Map[LogicalVariable, Expression] = groupingExpressions ++ aggregationExpressions

  override def keySet: Set[LogicalVariable] = groupingExpressions.keySet ++ aggregationExpressions.keySet

  override def dependingExpressions: Iterable[Expression] = super.dependingExpressions ++ aggregationExpressions.values

  override def withAddedProjections(groupingKeys: Map[LogicalVariable, Expression]): AggregatingQueryProjection =
    copy(groupingExpressions = this.groupingExpressions ++ groupingKeys)

  override def withPagination(queryPagination: QueryPagination): AggregatingQueryProjection =
    copy(queryPagination = queryPagination)

  override def exposedSymbols(coveredIds: Set[LogicalVariable]): Set[LogicalVariable] =
    groupingExpressions.keySet ++ aggregationExpressions.keySet

  override def withSelection(selections: Selections): QueryProjection = copy(selections = selections)
}

final case class DistinctQueryProjection(
  groupingExpressions: Map[LogicalVariable, Expression] = Map.empty,
  queryPagination: QueryPagination = QueryPagination.empty,
  selections: Selections = Selections(),
  isTerminating: Boolean = false
) extends QueryProjection {

  def projections: Map[LogicalVariable, Expression] = groupingExpressions

  def keySet: Set[LogicalVariable] = groupingExpressions.keySet

  override def withIsTerminating(boolean: Boolean): DistinctQueryProjection =
    copy(isTerminating = boolean)

  override def withAddedProjections(groupingKeys: Map[LogicalVariable, Expression]): DistinctQueryProjection =
    copy(groupingExpressions = this.groupingExpressions ++ groupingKeys)

  override def withPagination(queryPagination: QueryPagination): DistinctQueryProjection =
    copy(queryPagination = queryPagination)

  override def exposedSymbols(coveredIds: Set[LogicalVariable]): Set[LogicalVariable] = groupingExpressions.keySet

  override def withSelection(selections: Selections): QueryProjection = copy(selections = selections)
}

case class CommandProjection(clause: CommandClause) extends QueryHorizon {

  override def exposedSymbols(coveredIds: Set[LogicalVariable]): Set[LogicalVariable] = {
    val columns = clause match {
      case t: CommandClause if t.yieldItems.nonEmpty =>
        t.yieldItems.map(_.aliasedVariable)
      case _ => clause.unfilteredColumns.columns.map(_.variable)
    }
    coveredIds ++ columns
  }

  override def dependingExpressions: Seq[Expression] = Seq()

  override def allHints: Set[Hint] = Set.empty

  override def withoutHints(hintsToIgnore: Set[Hint]): QueryHorizon = this

  override def isTerminatingProjection: Boolean = false
}

abstract class AbstractProcedureCallProjection extends QueryHorizon
