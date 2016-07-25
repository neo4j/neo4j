/*
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
package org.neo4j.cypher.internal.compiler.v3_1

/**
 * This class defines the query planners used by cyphers.
 **/
sealed abstract class PlannerName {
  def name: String
  def toTextOutput: String
}

sealed abstract class CostBasedPlannerName extends PlannerName {
  val toTextOutput = "COST"
}

object CostBasedPlannerName {
  // This is the defining place for default used cost planner
  def default = IDPPlannerName
}

/**
 * Rule based query planner, default in all versions below 2.2
 */
case object RulePlannerName extends PlannerName {
  val name = "RULE"
  override def toTextOutput = name
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

object PlannerName {

  def apply(name: String): PlannerName = name.toUpperCase match {
    case RulePlannerName.name => RulePlannerName
    case IDPPlannerName.name => IDPPlannerName
    case DPPlannerName.name => DPPlannerName
    case "COST" => CostBasedPlannerName.default
    case "DEFAULT" => CostBasedPlannerName.default

    // Note that conservative planner is not exposed to end users.
    case n => throw new IllegalArgumentException(
      s"""$n is not a a valid planner, valid options are
         |${IDPPlannerName.name}, ${DPPlannerName.name} and ${RulePlannerName.name}""".stripMargin)
  }
}
