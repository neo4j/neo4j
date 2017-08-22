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

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.NewPlannerTestSupport

class VarLengthExpandQueryPlanAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("Plan should have right relationship direction") {
    setUp("From")
    val query = "PROFILE MATCH (a:From {name:'Keanu Reeves'})-[*..4]->(e:To {name:'Andres'}) RETURN *"
    val result = executeWithCostPlannerAndInterpretedRuntimeOnly(query)

    result should havePlanLike(
      """
        |Compiler CYPHER 3.3
        |
        |Planner COST
        |
        |Runtime ENTERPRISE-INTERPRETED
        |
        |+-----------------------+----------------+------+---------+-----------------+-------------------+----------------------+------------------+----------------------------------+
        || Operator              | Estimated Rows | Rows | DB Hits | Page Cache Hits | Page Cache Misses | Page Cache Hit Ratio | Variables        | Other                            |
        |+-----------------------+----------------+------+---------+-----------------+-------------------+----------------------+------------------+----------------------------------+
        || +ProduceResults       |              0 |    1 |       0 |               0 |                 0 |               0.0000 | anon[37], a, e   |                                  |
        || |                     +----------------+------+---------+-----------------+-------------------+----------------------+------------------+----------------------------------+
        || +Filter               |              0 |    1 |       5 |               0 |                 0 |               0.0000 | anon[37], a, e   | a:From; a.name = {  AUTOSTRING0} |
        || |                     +----------------+------+---------+-----------------+-------------------+----------------------+------------------+----------------------------------+
        || +VarLengthExpand(All) |              0 |    4 |       8 |               0 |                 0 |               0.0000 | anon[37], a -- e | (e)<-[:*..4]-(a)                 |
        || |                     +----------------+------+---------+-----------------+-------------------+----------------------+------------------+----------------------------------+
        || +Filter               |              0 |    1 |       1 |               0 |                 0 |               0.0000 | e                | e.name = {  AUTOSTRING1}         |
        || |                     +----------------+------+---------+-----------------+-------------------+----------------------+------------------+----------------------------------+
        || +NodeByLabelScan      |              1 |    1 |       2 |               0 |                 0 |               0.0000 | e                | :To                              |
        |+-----------------------+----------------+------+---------+-----------------+-------------------+----------------------+------------------+----------------------------------+
        |""".stripMargin)
  }

  test("Plan should have right relationship direction other direction") {
    setUp("To")
    val query = "PROFILE MATCH (a:From {name:'Keanu Reeves'})-[*..4]->(e:To {name:'Andres'}) RETURN *"
    val result = executeWithCostPlannerAndInterpretedRuntimeOnly(query)
    result should havePlanLike(
      """
        |Compiler CYPHER 3.3
        |
        |Planner COST
        |
        |Runtime ENTERPRISE-INTERPRETED
        |
        |+-----------------------+----------------+------+---------+-----------------+-------------------+----------------------+------------------+--------------------------------+
        || Operator              | Estimated Rows | Rows | DB Hits | Page Cache Hits | Page Cache Misses | Page Cache Hit Ratio | Variables        | Other                          |
        |+-----------------------+----------------+------+---------+-----------------+-------------------+----------------------+------------------+--------------------------------+
        || +ProduceResults       |              0 |    1 |       0 |               0 |                 0 |               0.0000 | anon[37], a, e   |                                |
        || |                     +----------------+------+---------+-----------------+-------------------+----------------------+------------------+--------------------------------+
        || +Filter               |              0 |    1 |       5 |               0 |                 0 |               0.0000 | anon[37], a, e   | e:To; e.name = {  AUTOSTRING1} |
        || |                     +----------------+------+---------+-----------------+-------------------+----------------------+------------------+--------------------------------+
        || +VarLengthExpand(All) |              0 |    4 |       8 |               0 |                 0 |               0.0000 | anon[37], e -- a | (a)-[:*..4]->(e)               |
        || |                     +----------------+------+---------+-----------------+-------------------+----------------------+------------------+--------------------------------+
        || +Filter               |              0 |    1 |       1 |               0 |                 0 |               0.0000 | a                | a.name = {  AUTOSTRING0}       |
        || |                     +----------------+------+---------+-----------------+-------------------+----------------------+------------------+--------------------------------+
        || +NodeByLabelScan      |              1 |    1 |       2 |               0 |                 0 |               0.0000 | a                | :From                          |
        |+-----------------------+----------------+------+---------+-----------------+-------------------+----------------------+------------------+--------------------------------+
        |""".stripMargin)
  }

  private def setUp(startLabel: String) {
    val a = createLabeledNode(Map("name" -> "Keanu Reeves"), "From")
    val b = createLabeledNode(Map("name" -> "Craig"), "User")
    val c = createLabeledNode(Map("name" -> "Olivia"), "User")
    val d = createLabeledNode(Map("name" -> "Carrie"), "User")
    val e = createLabeledNode(Map("name" -> "Andres"), "To")
    // Ensure compiler prefers to start at low cardinality 'To' node
    Range(0, 100).foreach(i => createLabeledNode(Map("name" -> s"node $i"), startLabel))
    relate(a, b)
    relate(b, c)
    relate(c, d)
    relate(d, e)
  }
}
