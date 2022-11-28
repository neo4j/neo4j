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

import org.neo4j.cypher.internal.ast.Hint
import org.neo4j.cypher.internal.ast.Union.UnionMapping

/**
 * A query in a representation that is consumed by the planner.
 */
trait PlannerQuery {
  def readOnly: Boolean
  def returns: Set[String]

  def allHints: Set[Hint]
  def withoutHints(hintsToIgnore: Set[Hint]): PlannerQuery
  def numHints: Int
  def visitHints[A](acc: A)(f: (A, Hint, QueryGraph) => A): A

  /**
   * @return all recursively included query graphs, with leaf information for Eagerness analysis.
   *         Query graphs from pattern expressions and pattern comprehensions will generate variable names that might clash with existing names, so this method
   *         is not safe to use for planning pattern expressions and pattern comprehensions.
   */
  def allQGsWithLeafInfo: collection.Seq[QgWithLeafInfo]

  /**
   * Use this method when you are certain that you are dealing with a SinglePlannerQuery, and not a UnionQuery.
   * It will throw an Exception if this is a UnionQuery.
   */
  def asSinglePlannerQuery: SinglePlannerQuery
}

/**
 * This represents the union of the queries.
 * @param lhs the first part, which can itself be either a UnionQuery or a SinglePlannerQuery
 * @param rhs the second part, which is a SinglePlannerQuery
 * @param distinct whether it is a distinct union
 * @param unionMappings mappings of return items from both parts
 */
case class UnionQuery(
  lhs: PlannerQuery,
  rhs: SinglePlannerQuery,
  distinct: Boolean,
  unionMappings: List[UnionMapping]
) extends PlannerQuery {
  override def readOnly: Boolean = lhs.readOnly && rhs.readOnly

  override def returns: Set[String] = lhs.returns.map { returnColInLhs =>
    unionMappings.collectFirst {
      case UnionMapping(varAfterUnion, varInLhs, _) if varInLhs.name == returnColInLhs => varAfterUnion.name
    }.get
  }

  override def allHints: Set[Hint] = lhs.allHints ++ rhs.allHints

  override def withoutHints(hintsToIgnore: Set[Hint]): PlannerQuery = copy(
    lhs = lhs.withoutHints(hintsToIgnore),
    rhs = rhs.withoutHints(hintsToIgnore)
  )

  override def numHints: Int = lhs.numHints + rhs.numHints

  override def visitHints[A](acc: A)(f: (A, Hint, QueryGraph) => A): A = {
    val queryAcc = rhs.visitHints(acc)(f)
    lhs.visitHints(queryAcc)(f)
  }

  override def asSinglePlannerQuery: SinglePlannerQuery =
    throw new IllegalStateException("Called asSinglePlannerQuery on a UnionQuery")

  override def allQGsWithLeafInfo: collection.Seq[QgWithLeafInfo] = lhs.allQGsWithLeafInfo ++ rhs.allQGsWithLeafInfo
}
