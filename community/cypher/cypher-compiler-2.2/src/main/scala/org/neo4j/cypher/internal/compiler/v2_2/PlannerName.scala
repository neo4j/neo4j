/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2

/**
 * This class defines the query planners used by cyphers.
 **/
sealed abstract class PlannerName(val name: String)

/**
 * Rule based query planner, default in all versions below 2.2
 */
case object Rule extends PlannerName("RULE")

/**
 * Cost based query planner uses statistics from the running database to find optimal
 * query execution plans.
 */
case object Cost extends PlannerName("COST")


/**
 * Hybrid planner that uses the Cost based planner for most of its operations but falls back to
 * Rule based planner for classes of queries where the cost based planner might end up with suboptimal plans.
 */
case object Conservative extends PlannerName("CONSERVATIVE")

object PlannerName {
  val default = Conservative
  def apply(name: String): PlannerName = name.toUpperCase match {
    case "RULE" => Rule
    case "COST" => Cost
    case "CONSERVATIVE" => Conservative
    //Note that conservative planner is not exposed to end users.
    case n => throw new IllegalArgumentException(s"$n is not a a valid planner, valid options are COST and RULE")
  }
}
