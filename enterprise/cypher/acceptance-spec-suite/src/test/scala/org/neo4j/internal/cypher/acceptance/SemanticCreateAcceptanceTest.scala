/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection
import org.neo4j.cypher.{ExecutionEngineFunSuite, PatternGen}
import org.neo4j.graphdb.ResourceIterator
import org.scalacheck.{Gen, Shrink}

/*
 * Tests create on random patterns.
 *  - makes sure that whatever pattern we create is returned when doing MATCH on pattern.
 */
class SemanticCreateAcceptanceTest extends ExecutionEngineFunSuite with PatternGen {

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
          graph.execute(s"CREATE $patternString")

          //find created pattern (cannot return * since everything might be unnamed)
          val result1 = graph.execute(s"MATCH $patternString RETURN 42")
          hasSingleRow(result1)
          val result2 = graph.execute(s"CYPHER runtime=interpreted MATCH $patternString RETURN 42")
          hasSingleRow(result2)

          //clean up
          graph.execute(s"MATCH (n) DETACH DELETE n")
        }
      }
    }
  }

  private def hasSingleRow(in: ResourceIterator[_]) = {
    in.hasNext should equal(true)
    in.next()
    in.hasNext should equal(false)
  }

  override protected def numberOfTestRuns: Int = 20

  override def relGen = Gen.oneOf(typedRelGen, namedTypedRelGen, typedWithPropertiesRelGen, namedTypedWithPropertiesRelGen)

  override def nodeGen = Gen.oneOf(emptyNodeGen, namedNodeGen, labeledNodeGen, namedLabeledNodeGen, labeledWithPropertiesNodeGen, namedLabeledWithPropertiesNodeGen)

  override def relDirection = Gen.oneOf(SemanticDirection.INCOMING, SemanticDirection.OUTGOING)
}
