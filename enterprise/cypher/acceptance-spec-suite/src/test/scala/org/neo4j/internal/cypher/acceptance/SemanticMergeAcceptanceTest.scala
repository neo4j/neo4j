/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection
import org.neo4j.cypher.internal.RewindableExecutionResult
import org.neo4j.cypher.{ExecutionEngineFunSuite, PatternGen, QueryStatisticsTestSupport}
import org.neo4j.graphdb.ResourceIterator
import org.scalacheck.{Gen, Shrink}

/*
 * Tests merge on random patterns.
 *  - makes sure that whatever pattern we create is returned when doing MATCH on pattern.
 */
class SemanticMergeAcceptanceTest
  extends ExecutionEngineFunSuite with PatternGen with QueryStatisticsTestSupport {

  //we don't want scala check to shrink patterns here and leave things in the database
  implicit val dontShrink: Shrink[List[Element]] = Shrink(s => Stream.empty)

  test("MERGE on empty database and then match") {
    forAll(patterns) { pattern =>
      // reset naming sequence number
      nameSeq.set(0)

      whenever(pattern.nonEmpty) {
        val patternString = pattern.map(_.string).mkString
        withClue(s"failing on pattern $patternString") {
          //update
          graph.execute(s"MERGE $patternString")

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

  test("CREATE and then MERGE should not update database") {
    forAll(patterns) { pattern =>
      // reset naming sequence number
      nameSeq.set(0)

      whenever(pattern.nonEmpty) {
        val patternString = pattern.map(_.string).mkString
        withClue(s"failing on pattern $patternString") {
          //update
          graph.execute(s"CREATE $patternString")

          //find created pattern (cannot return * since everything might be unnamed)
          val result = RewindableExecutionResult(graph.execute(s"MERGE $patternString RETURN 42"))
          result.toList should have size 1
          assertStats(result, nodesCreated = 0)

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

  override def relGen = Gen
    .oneOf(typedRelGen, namedTypedRelGen, typedWithPropertiesRelGen, namedTypedWithPropertiesRelGen)

  override def nodeGen = Gen
    .oneOf(emptyNodeGen, namedNodeGen, labeledNodeGen, namedLabeledNodeGen, labeledWithPropertiesNodeGen,
      namedLabeledWithPropertiesNodeGen)

  override def relDirection = Gen.oneOf(SemanticDirection.INCOMING, SemanticDirection.OUTGOING)
}
