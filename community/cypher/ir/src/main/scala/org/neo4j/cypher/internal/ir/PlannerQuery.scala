/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.cypher.internal.v4_0.ast.Hint
import org.neo4j.cypher.internal.v4_0.ast.Union.UnionMapping

/**
 * A query in a representation that is consumed by the planner.
 */
case class PlannerQuery(query: PlannerQueryPart, periodicCommit: Option[PeriodicCommit]) {
  def readOnly: Boolean = query.readOnly
}

/**
 * A part of a PlannerQuery.
 */
trait PlannerQueryPart {
  def readOnly: Boolean
  def returns: Set[String]

  def allHints: Set[Hint]
  def withoutHints(hintsToIgnore: Set[Hint]): PlannerQueryPart
  def numHints: Int

  /**
   * Use this method when you are certain that you are dealing with a SinglePlannerQuery, and not a UnionQuery.
   * It will throw an Exception if this is a UnionQuery.
   */
  def asSinglePlannerQuery: SinglePlannerQuery
}

/**
 * This represents the union of the queries.
 * @param part the first part, which can itself be either a UnionQuery or a SinglePlannerQuery
 * @param query the second part, which is a SinglePlannerQuery
 * @param distinct whether it is a distinct union
 * @param unionMappings mappings of return items from both parts
 */
case class UnionQuery(part: PlannerQueryPart,
                      query: SinglePlannerQuery,
                      distinct: Boolean,
                      unionMappings: List[UnionMapping]) extends PlannerQueryPart {
  override def readOnly: Boolean = part.readOnly && query.readOnly

  override def returns: Set[String] = part.returns.map { returnColInPart =>
    unionMappings.collectFirst {
      case UnionMapping(varAfterUnion, varInPart, _) if varInPart.name == returnColInPart => varAfterUnion.name
    }.get
  }

  override def allHints: Set[Hint] = part.allHints ++ query.allHints

  override def withoutHints(hintsToIgnore: Set[Hint]): PlannerQueryPart = copy(
    part = part.withoutHints(hintsToIgnore),
    query = query.withoutHints(hintsToIgnore)
  )

  override def numHints: Int = part.numHints + query.numHints

  override def asSinglePlannerQuery: SinglePlannerQuery = throw new IllegalStateException("Called asSinglePlannerQuery on a UnionQuery")
}
