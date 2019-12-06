/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.v4_0.util.attribution.SameId
import org.neo4j.cypher.internal.v4_0.util.{Rewriter, bottomUp}

/**
  * Rewrite cartesian products of index seeks into a specialized multiple index seek operator
  */
case object combineCartesianProductOfMultipleIndexSeeks extends Rewriter {

  private val instance: Rewriter = bottomUp(Rewriter.lift {
    case o @ CartesianProduct(lhs: IndexSeekLeafPlan, rhs: IndexSeekLeafPlan) =>
      MultiNodeIndexSeek(Array(lhs, rhs))(SameId(o.id))

    case o @ CartesianProduct(lhs: MultiNodeIndexSeek, rhs: IndexSeekLeafPlan) =>
      MultiNodeIndexSeek(lhs.nodeIndexSeeks :+ rhs)(SameId(o.id))

    case o @ CartesianProduct(lhs: IndexSeekLeafPlan, rhs: MultiNodeIndexSeek) =>
      MultiNodeIndexSeek(lhs +: rhs.nodeIndexSeeks)(SameId(o.id))
  })

  override def apply(input: AnyRef): AnyRef = instance.apply(input)
}
