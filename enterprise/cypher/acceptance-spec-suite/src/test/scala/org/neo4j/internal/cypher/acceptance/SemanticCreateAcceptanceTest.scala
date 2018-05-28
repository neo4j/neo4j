/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.frontend.v3_2.SemanticDirection
import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport, PatternGen}
import org.scalacheck.{Gen, Shrink}

/*
 * Tests create on random patterns.
 *  - uses updateWithBothPlanners to verify that the statistics match the rule planner
 *  - makes sure that whatever pattern we create is returned when doing MATCH on pattern.
 */
class SemanticCreateAcceptanceTest extends ExecutionEngineFunSuite with PatternGen with NewPlannerTestSupport {

  //we don't want scala check to shrink patterns here and leave things in the database
  implicit val dontShrink: Shrink[List[Element]] = Shrink(s => Stream.empty)

  test("create and match random patterns") {
    forAll(patterns) { pattern =>
      // reset naming sequence number
      nameSeq.set(0)


      whenever(pattern.nonEmpty) {
        val patternString = pattern.map(_.string).mkString
        withClue(s"failing on pattern $patternString") {
          //update
          updateWithBothPlannersAndCompatibilityMode(s"CREATE $patternString")

          //find created pattern (cannot return * since everything might be unnamed)
          val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode(s"MATCH $patternString RETURN 42")
          result.toList should have size 1

          //clean up
          updateWithBothPlannersAndCompatibilityMode(s"MATCH (n) DETACH DELETE n")
        }
      }
    }
  }

  override protected def numberOfTestRuns: Int = 20

  override def relGen = Gen.oneOf(typedRelGen, namedTypedRelGen, typedWithPropertiesRelGen, namedTypedWithPropertiesRelGen)

  override def nodeGen = Gen.oneOf(emptyNodeGen, namedNodeGen, labeledNodeGen, namedLabeledNodeGen, labeledWithPropertiesNodeGen, namedLabeledWithPropertiesNodeGen)

  override def relDirection = Gen.oneOf(SemanticDirection.INCOMING, SemanticDirection.OUTGOING)
}
