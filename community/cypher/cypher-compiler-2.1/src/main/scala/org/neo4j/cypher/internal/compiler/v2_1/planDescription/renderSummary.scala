/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planDescription

import org.neo4j.cypher.internal.compiler.v2_1.planDescription.PlanDescription.Arguments.DbHits

object renderSummary extends (PlanDescription => String) {
  def apply(plan: PlanDescription): String =
    "Total database accesses: " +
    plan.toSeq.
      map(extractDbHits).
      reduce(optionallyAddTogether).
      map(_.toString).
      getOrElse("?")

  private def optionallyAddTogether(a: Option[Long], b: Option[Long]): Option[Long] = for (a0 <- a; b0 <- b) yield a0 + b0

  private def extractDbHits(pl: PlanDescription): Option[Long] = pl.arguments.collectFirst {
    case DbHits(x) => x
  }
}
