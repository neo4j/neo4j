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

import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport, QueryStatisticsTestSupport}
import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._

class PlanningPerformanceAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with NewPlannerTestSupport with TimeLimitedTests {

  override def timeLimit: Span = 10 seconds

  //see https://github.com/neo4j/neo4j/issues/7407
  test("should plan within reasonable time") {
    updateWithBothPlannersAndCompatibilityMode(
      """CREATE (n1: `node1` {oid: '1'})
        |CREATE (n2: `node2` {oid: '2'})
        |CREATE (n3: `node3` {oid: '3'})
        |CREATE (n4: `node4` {oid: '4'})
        |CREATE (n5: `node5` {oid: '5'})
        |CREATE (n6: `node6` {oid: '6'})
        |CREATE (n7: `node7` {oid: '7'})
        |CREATE (n8: `users` {oid: '8'})
        |CREATE (n9: `user` {oid: '9'})
        |CREATE (n1)-[r10:`HAS_CHILD` {oid: '10'}]->(n2)
        |CREATE (n1)-[r11:`HAS_CHILD` {oid: '11'}]->(n3)
        |CREATE (n2)-[r12:`HAS_CHILD` {oid: '12'}]->(n4)
        |CREATE (n2)-[r13:`HAS_CHILD` {oid: '13'}]->(n5)
        |CREATE (n3)-[r14:`HAS_CHILD` {oid: '14'}]->(n6)
        |CREATE (n3)-[r15:`HAS_CHILD` {oid: '15'}]->(n7)
        |CREATE (n8)-[r16:`SECURITY` {oid: '16'}]->(n3)
        |CREATE (n9)-[r17:`PART_OF` {oid: '17'}]->(n8)""".stripMargin)

    val query =
      """OPTIONAL MATCH (n0 { oid: '1'})
        |OPTIONAL MATCH (n1 { oid: '2'})
        |OPTIONAL MATCH (n2 { oid: '3'})
        |OPTIONAL MATCH (n3 { oid: '4'})
        |OPTIONAL MATCH (n4 { oid: '5'})
        |OPTIONAL MATCH (n5 { oid: '6'})
        |OPTIONAL MATCH (n6 { oid: '7'})
        |OPTIONAL MATCH (n7 { oid: '8'})
        |OPTIONAL MATCH (n8 { oid: '9'})
        |OPTIONAL MATCH ( { oid : '1'})-[r0 { oid: '10'}]-( { oid : '2'})
        |OPTIONAL MATCH ( { oid : '1'})-[r1 { oid: '11'}]-( { oid : '3'})
        |OPTIONAL MATCH ( { oid : '2'})-[r2 { oid: '12'}]-( { oid : '4'})
        |OPTIONAL MATCH ( { oid : '2'})-[r3 { oid: '13'}]-( { oid : '5'})
        |OPTIONAL MATCH ( { oid : '3'})-[r4 { oid: '14'}]-( { oid : '6'})
        |OPTIONAL MATCH ( { oid : '3'})-[r5 { oid: '15'}]-( { oid : '7'})
        |OPTIONAL MATCH ( { oid : '8'})-[r6 { oid: '16'}]-( { oid : '3'})
        |OPTIONAL MATCH ( { oid : '9'})-[r7 { oid: '17'}]-( { oid : '8'})
        |DETACH DELETE n0, n1, n2, n3, n4, n5, n6, n7, n8 DELETE r0, r1, r2, r3, r4, r5, r6, r7""".stripMargin

    updateWithBothPlannersAndCompatibilityMode(s"EXPLAIN $query")
  }

}
