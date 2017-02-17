/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.frontend.v3_2.SemanticDirection
import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport, PatternGen}
import org.scalacheck.{Gen, Shrink}

/*
 * Creates a random pattern, matches on it and deletes all variables
 *  - uses updateWithBothPlanners to verify that the statistics match the rule planner
 *  - when done the database should be empty.
 */
class SemanticDeleteAcceptanceTest extends ExecutionEngineFunSuite with PatternGen with NewPlannerTestSupport {

  //we don't want scala check to shrink patterns here and leave things in the database
  implicit val dontShrink: Shrink[List[Element]] = Shrink(s => Stream.empty)

  test("match and delete random patterns") {

    forAll(patterns) { pattern =>
      // reset naming sequence number
      nameSeq.set(0)

      whenever(pattern.nonEmpty) {
        val patternString = pattern.map(_.string).mkString
        withClue(s"failing on pattern $patternString") {
          //update
          updateWithBothPlannersAndCompatibilityMode(s"CREATE $patternString")
          //delete
          val variables = findAllRelationshipNames(pattern) ++ findAllNodeNames(pattern)
          updateWithBothPlannersAndCompatibilityMode(s"MATCH $patternString DELETE ${variables.mkString(",")} RETURN count(*)")

          //now db should be empty
          executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH () RETURN count(*) AS c").toList should equal(List(Map("c" -> 0)))
        }
      }
    }
  }

  test("match and detach delete random patterns") {

    forAll(patterns) { pattern =>
      // reset naming sequence number
      nameSeq.set(0)

      whenever(pattern.nonEmpty) {
        val patternString = pattern.map(_.string).mkString
        withClue(s"failing on pattern $patternString") {
          //update
          updateWithBothPlannersAndCompatibilityMode(s"CREATE $patternString")
          //delete
          val variables = findAllNodeNames(pattern)
          updateWithBothPlanners(s"MATCH $patternString DETACH DELETE ${variables.mkString(",")} RETURN count(*)")

          //now db should be empty
          executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH () RETURN count(*) AS c").toList should equal(List(Map("c" -> 0)))
        }
      }
    }
  }

  test("undirected match and delete random patterns") {

    forAll(patterns) { pattern =>
      // reset naming sequence number
      nameSeq.set(0)

      whenever(pattern.nonEmpty) {
        val patternString = pattern.map(_.string).mkString
        withClue(s"failing on pattern $patternString") {
          //update
          updateWithBothPlannersAndCompatibilityMode(s"CREATE $patternString")
          //delete
          val variables = findAllRelationshipNames(pattern) ++ findAllNodeNames(pattern)
          val undirected = makeUndirected(pattern).map(_.string).mkString
          updateWithBothPlannersAndCompatibilityMode(s"MATCH $undirected DELETE ${variables.mkString(",")}")

          //now db should be empty
          executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH () RETURN count(*) AS c").toList should equal(List(Map("c" -> 0)))
        }
      }
    }
  }

  test("undirected match and detach delete random patterns") {

    forAll(patterns) { pattern =>
      // reset naming sequence number
      nameSeq.set(0)

      whenever(pattern.nonEmpty) {
        val patternString = pattern.map(_.string).mkString
        withClue(s"failing on pattern $patternString") {
          //update
          updateWithBothPlannersAndCompatibilityMode(s"CREATE $patternString")
          //delete
          val variables = findAllNodeNames(pattern)
          val undirected = makeUndirected(pattern).map(_.string).mkString
          updateWithBothPlannersAndCompatibilityMode(s"MATCH $undirected DETACH DELETE ${variables.mkString(",")}")

          //now db should be empty
          executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH () RETURN count(*) AS c").toList should equal(List(Map("c" -> 0)))
        }
      }
    }
  }

  private def makeUndirected(elements: Seq[Element]): Seq[Element] = elements.map {
    case n: NodeWithRelationship => n.copy(rel = n.rel.withDirection(SemanticDirection.BOTH))
    case other => other
  }

  override protected def numberOfTestRuns: Int = 20

  override def relGen = Gen.oneOf(namedTypedRelGen, namedTypedWithPropertiesRelGen)

  override def nodeGen = Gen.oneOf(namedNodeGen, namedLabeledNodeGen, namedLabeledWithPropertiesNodeGen)

  override def relDirection = Gen.oneOf(SemanticDirection.INCOMING, SemanticDirection.OUTGOING)
}
