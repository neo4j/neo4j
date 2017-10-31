/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.planner.v3_4.spi

import org.neo4j.cypher.internal.frontend.v3_4.PlannerName

sealed abstract class CostBasedPlannerName extends PlannerName {
  val toTextOutput = "COST"
}

object CostBasedPlannerName {
  // This is the defining place for default used cost planner
  def default = IDPPlannerName
}

/**
 * Cost based query planner uses statistics from the running database to find good
 * query execution plans using limited exhaustive search based on the IDP algorithm.
 */
case object IDPPlannerName extends CostBasedPlannerName {
  val name = "IDP"
}

/**
 * Cost based query planner uses statistics from the running database to find good
 * query execution plans using exhaustive search based on the DP algorithm.
 */
case object DPPlannerName extends CostBasedPlannerName {
  val name = "DP"
}

/**
  * Queries that doesn't require planning are dealt with by a separate planning step
  */
case object ProcedurePlannerName extends PlannerName {
  val name = "PROCEDURE"

  override def toTextOutput: String = "PROCEDURE"
}

object PlannerNameFor {

  def apply(name: String): PlannerName = name.toUpperCase match {
    case IDPPlannerName.name => IDPPlannerName
    case DPPlannerName.name => DPPlannerName
    case "COST" => CostBasedPlannerName.default
    case "DEFAULT" => CostBasedPlannerName.default

    // Note that conservative planner is not exposed to end users.
    case n => throw new IllegalArgumentException(
      s"$n is not a a valid planner, valid options are COST, ${IDPPlannerName.name} and ${DPPlannerName.name}")
  }
}
