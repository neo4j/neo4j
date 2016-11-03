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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.compiler.v3_2.planDescription.InternalPlanDescription
import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport}
import org.scalatest.matchers.{MatchResult, Matcher}

class StatisticsPlanningAcceptanceTest  extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("planning should not take into account transaction state when counting nodes") {
    graph.createIndex("User", "name")

    createLabeledNode(Map("name" -> "Mats"), "User")

    graph.inTx {
      (0 to 10).foreach { _ =>
        createLabeledNode("User")
      }
      executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (n:User { name: 'Mats' }) RETURN n").executionPlanDescription() should includeOperation(
        "NodeByLabelScan")
    }
  }

  case class includeOperation(operationName: String) extends Matcher[InternalPlanDescription] {

    override def apply(result: InternalPlanDescription): MatchResult = {
      val operationExists = result.flatten.exists { description =>
        description.name == operationName
      }

      MatchResult(operationExists, matchResultMsg(negated = false, result), matchResultMsg(negated = true, result))
    }

    private def matchResultMsg(negated: Boolean, result: InternalPlanDescription) =
      s"$operationName ${if (negated) "" else "not"} found in plan description\n $result"
  }

}
