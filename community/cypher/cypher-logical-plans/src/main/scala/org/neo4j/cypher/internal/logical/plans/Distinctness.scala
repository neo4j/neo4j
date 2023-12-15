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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.IntegerLiteral
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.util.InputPosition

object Distinctness {

  /**
   * A lot of binary plans share the same logic for distinctness. Not all of them.
   */
  def distinctColumnsOfBinaryPlan(
    left: LogicalPlan,
    right: LogicalPlan
  ): Distinctness = {
    (left.distinctness, right.distinctness) match {
      case (DistinctColumns(leftColumns), DistinctColumns(rightColumns)) =>
        DistinctColumns(leftColumns ++ rightColumns)
      case (dcLeft: DistinctColumns, AtMostOneRow)  => dcLeft
      case (AtMostOneRow, dcRight: DistinctColumns) => dcRight
      case (AtMostOneRow, AtMostOneRow)             => AtMostOneRow
      case _                                        => NotDistinct
    }
  }

  def distinctColumnsOfDistinct(
    left: LogicalPlan,
    groupingExpressions: Map[LogicalVariable, Expression]
  ): Distinctness = {
    val distinctColumnsFromDistinct = groupingExpressions.keySet
    left.distinctness match {
      case AtMostOneRow => AtMostOneRow
      case DistinctColumns(previouslyDistinctColumns) =>
        val newDistinctColumns =
          if (previouslyDistinctColumns.subsetOf(distinctColumnsFromDistinct)) previouslyDistinctColumns
          else distinctColumnsFromDistinct
        DistinctColumns(newDistinctColumns)
      case NotDistinct => DistinctColumns(distinctColumnsFromDistinct)
    }
  }

  def distinctColumnsOfAggregation(groupingKeys: Set[LogicalVariable]): Distinctness = {
    if (groupingKeys.isEmpty) {
      // Aggregation without grouping outputs at most 1 row
      AtMostOneRow
    } else {
      DistinctColumns(groupingKeys)
    }
  }

  def distinctColumnsOfLimit(limitExpr: Expression, source: LogicalPlan): Distinctness = {
    limitExpr match {
      case i: IntegerLiteral if i.value == 0 || i.value == 1 => AtMostOneRow
      case _                                                 => source.distinctness
    }
  }
}

sealed trait Distinctness {

  /**
   * Checks whether this Distinctness covers a set of given expressions that should become distinct.
   */
  def covers(expressions: Iterable[Expression]): Boolean

  /**
   * Return a new Distinctness with distinct columns renamed according to the given projectExpressions.
   */
  def renameColumns(projectExpressions: Map[LogicalVariable, Expression]): Distinctness
}

/**
 * When we are not sure if there is at most one row, but there are some distinct columns.
 * 
 * @param columns the tuple of `columns` is distinct.
 */
case class DistinctColumns private (columns: Set[LogicalVariable]) extends Distinctness {
  AssertMacros.checkOnlyWhenAssertionsAreEnabled(columns.nonEmpty, "DistinctColumns must be non-empty")

  /**
   * If these distinct columns are a subset of the given distinct `expressions`
   * then return {{{true}}}.
   * Distinctness over these columns then implies distinctness over the expressions given.
   *
   * {{{DISTINCT a =covers=> a, b, ...}}}
   * {{{DISTINCT a =implies=> DISTINCT a, b, ...}}}
   */
  override def covers(expressions: Iterable[Expression]): Boolean = {
    val vars = expressions.collect {
      case lv: LogicalVariable => lv
    }
    columns.subsetOf(vars.toSet)
  }

  override def renameColumns(projectExpressions: Map[LogicalVariable, Expression]): Distinctness = {
    DistinctColumns(columns.map {
      case lv @ LogicalVariable(varName) =>
        projectExpressions.collectFirst {
          case (newVar, Variable(`varName`)) => newVar
        }.getOrElse(lv)
    })
  }
}

object DistinctColumns {

  def apply(columns: LogicalVariable*): Distinctness = DistinctColumns(columns.toSet)

  def apply(columns: Set[LogicalVariable]): Distinctness =
    if (columns.isEmpty) {
      NotDistinct
    } else {
      new DistinctColumns(columns)
    }

  def apply(columnName: String): Distinctness = DistinctColumns(Variable(columnName)(InputPosition.NONE))

}

/**
 *  If we are certain that the operator returns at most one row.
 */
case object AtMostOneRow extends Distinctness {
  override def covers(expressions: Iterable[Expression]): Boolean = true

  override def renameColumns(projectExpressions: Map[LogicalVariable, Expression]): Distinctness = AtMostOneRow
}

case object NotDistinct extends Distinctness {
  override def covers(expressions: Iterable[Expression]): Boolean = false

  override def renameColumns(projectExpressions: Map[LogicalVariable, Expression]): Distinctness = NotDistinct
}
