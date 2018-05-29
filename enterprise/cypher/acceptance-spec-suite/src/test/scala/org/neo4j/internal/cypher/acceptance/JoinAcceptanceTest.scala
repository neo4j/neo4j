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

import org.neo4j.cypher._

class JoinAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport {

  test("optional match join should not crash") {
    val query =
      """MATCH (a:A)-->(b:B)-->(c:C)
        |OPTIONAL MATCH (h)<--(g:G)<--(c)
        |USING JOIN ON c
        |RETURN a,b,c,g,h""".stripMargin
    graph.execute(query) // should not crash
  }

  test("larger optional match join should not crash") {
    val query =
      """MATCH (b:B)-->(c:C)
        |OPTIONAL MATCH (c)<--(d:D)
        |USING JOIN ON c
        |OPTIONAL MATCH (g:G)<--(c)
        |USING JOIN ON c
        |RETURN b,c,d,g""".stripMargin
    graph.execute(query) // should not crash
  }

  test("unfulfillable join hint should not crash") {
    // can use join on (b,c) only
    val query =
      """
        |MATCH (b:B)
        |MATCH (c:C)
        |OPTIONAL MATCH (c)-->(a:A)<--(b)
        |USING JOIN ON c
        |RETURN a,b,c""".stripMargin
    graph.execute(query) // should not crash
  }

  test("order in which join hints are solved should not matter") {
    val query =
      """MATCH (a)-[:X]->(b)-[:X]->(c)-[:X]->(d)-[:X]->(e)
        |USING JOIN ON b
        |USING JOIN ON c
        |USING JOIN ON d
        |WHERE a.prop = e.prop
        |RETURN b, d""".stripMargin
    graph.execute(query) // should not crash
  }
}
