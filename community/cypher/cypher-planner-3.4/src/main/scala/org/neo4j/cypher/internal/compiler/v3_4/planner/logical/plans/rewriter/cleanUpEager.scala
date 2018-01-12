/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.Solveds
import org.neo4j.cypher.internal.util.v3_4.attribution.{Attributes, SameId}
import org.neo4j.cypher.internal.util.v3_4.{Rewriter, bottomUp}
import org.neo4j.cypher.internal.v3_4.logical.plans.{Eager, Limit, LoadCSV, UnwindCollection}

case class cleanUpEager(solveds: Solveds, attributes: Attributes) extends Rewriter {

  private val instance: Rewriter = bottomUp(Rewriter.lift {

    // E E L => E L
    case eager@Eager(Eager(source)) =>
      eager.copy(source = source)(SameId(eager.id))

    // E U => U E
    case eager@Eager(unwind@UnwindCollection(source, _, _)) =>
      val res = unwind.copy(source = eager.copy(source = source)(SameId(eager.id)))(attributes.copy(unwind.id))
      solveds.copy(eager.id, res.id)
      res

    // E LCSV => LCSV E
    case eager@Eager(loadCSV@LoadCSV(source, _, _, _, _, _)) =>
      val res = loadCSV.copy(source = eager.copy(source = source)(SameId(eager.id)))(attributes.copy(loadCSV.id))
      solveds.copy(eager.id, res.id)
      res

    // LIMIT E => E LIMIT
    case limit@Limit(eager@Eager(source), _, _8) =>
      val res = eager.copy(source = limit.copy(source = source)(SameId(limit.id)))(attributes.copy(eager.id))
      solveds.copy(limit.id, res.id)
      res
  })

  override def apply(input: AnyRef): AnyRef = instance.apply(input)
}
