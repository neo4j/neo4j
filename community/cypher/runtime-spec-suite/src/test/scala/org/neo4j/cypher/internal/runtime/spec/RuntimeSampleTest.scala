/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.spec

import org.neo4j.cypher.internal.InterpretedRuntime
import org.neo4j.cypher.internal.runtime.QueryStatistics

/**
  * Sample tests to demonstrate the runtime acceptance test framework. Remove eventually?
  */
class RuntimeSampleTest extends RuntimeTestSuite(COMMUNITY_EDITION, InterpretedRuntime)
{

  test("sample test I - simple all nodes scan") {
    // given
    val nodes = nodeGraph(10)

    // when
    val logicalQuery = new LogicalQueryBuilder(graphDb)
      .produceResults("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withSingleValueRows(nodes)
  }

  test("sample test II - logical plan with branches") {
    // given
    val nodes = nodeGraph(2)

    // when
    val logicalQuery = new LogicalQueryBuilder(graphDb)
      .produceResults("x", "y", "z", "q")
      .apply()
      .|.apply()
      .|.|.allNodeScan("q")
      .|.allNodeScan("z")
      .apply()
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
        x <- nodes
        y <- nodes
        z <- nodes
        q <- nodes
      } yield Array(x, y, z, q)
    runtimeResult should beColumns("x", "y", "z", "q").withRows(expected)
  }

  test("samlpe test III - more complex stuff") {
    // given
    val q =  """
               |CREATE (p:Person{uuid:"XYZ"})-[:T]->(t:Token)-[:HAS_TOKEN_DATA]->(td:TokenData)
               |CREATE (p)-[:P]->(pd:PersonData)
               |CREATE (p)-[investigated:IS_BEING_INVESTIGATED]->(agent:Agent)
               |CREATE (p)-[investigated2:WAS_INVESTIGATED]->(agent2:Agent)
               |CREATE (p)-[:E]->(e:Email)
               |CREATE (p)-[:A]->(a:Address)
               |CREATE (p)-[:S]->(ssn:TemporarySsn)-[:D]->(doc:PhysicalDocument)
               |CREATE (p)-[:S]->(ssn2:InternallyVerifiedSsn)-[:D]->(doc2:PhysicalDocument)
               |CREATE (p)-[:S]->(ssn3:Ssn)-[:D]->(doc3:PhysicalDocument)
               |CREATE (doc)-[:V]->(procInfo:VerificationProcessInfo)
               |CREATE (doc2)-[:V]->(procInfo2:VerificationProcessInfo)
               |CREATE (doc3)-[:V]->(procInfo3:VerificationProcessInfo)
             """.stripMargin
    graphDb.execute(q)

    uniqueIndex("Person", "uuid")

    // when
    val logicalQuery = new LogicalQueryBuilder(graphDb)
      .produceResults()
      .emptyResult()
      .detachDeleteNode("p")
      .detachDeleteNode("procInfo")
      .detachDeleteNode("doc")
      .detachDeleteNode("ssn")
      .eager()
      .apply()
      .|.optionalExpand("(doc)-->(procInfo)", "procInfo:VerificationProcessInfo")
      .|.optionalExpand("(ssn)-->(doc)", "doc:PhysicalDocument")
      .|.optional()
      .|.expandInto("(p)-->(ssn)")
      .|.distinct("p AS p", "ssn AS ssn")
      .|.union()
      .|.|.nodeByLabelScan("ssn", "Ssn", "p")
      .|.union()
      .|.|.nodeByLabelScan("ssn", "InternallyVerifiedSsn", "p")
      .|.nodeByLabelScan("ssn", "TemporarySsn", "p")
      .eager()
      .detachDeleteNode("a")
      .eager()
      .apply()
      .|.optional()
      .|.expandInto("(p)-->(a)")
      .|.nodeByLabelScan("a", "Address", "p")
      .eager()
      .deleteRel("investigated")
      .eager()
      .optionalExpand("(p)-[investigated:IS_BEING_INVESTIGATED|WAS_INVESTIGATED]->(agent)", "agent:Agent")
      .eager()
      .detachDeleteNode("pd")
      .eager()
      .apply()
      .|.optional()
      .|.expandInto("(p)-->(pd)")
      .|.nodeByLabelScan("pd", "PersonData", "p")
      .eager()
      .detachDeleteNode("td")
      .detachDeleteNode("t")
      .eager()
      .optionalExpand("(t)-[:HAS_TOKEN_DATA]->(td)", "td:TokenData")
      .optionalExpand("(p)-->(t)", "t:Token")
      .nodeIndexOperator("p:Person(uuid = 'XYZ')", unique = true)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns().withNoRows().withStatistics(QueryStatistics(nodesDeleted = 14, relationshipsDeleted = 16))
  }
}
