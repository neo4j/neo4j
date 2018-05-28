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

import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport}

class VarLengthExpandQueryPlanAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("Plan should have right relationship direction") {
    setUp("From")
    val query = "PROFILE MATCH (a:From {name:'Keanu Reeves'})-[*..4]->(e:To {name:'Andres'}) RETURN *"
    val result = executeWithCostPlannerAndInterpretedRuntimeOnly(query)

    result should havePlanLike(
      """
        |+-----------------------+----------------+------+---------+-----------------+-------------------+------------------+-----------------------------------+
        || Operator              | Estimated Rows | Rows | DB Hits | Page Cache Hits | Page Cache Misses | Variables        | Other                             |
        |+-----------------------+----------------+------+---------+-----------------+-------------------+------------------+-----------------------------------+
        || +ProduceResults       |              0 |    1 |       0 |               0 |                 0 | anon[37], a, e   |                                   |
        || |                     +----------------+------+---------+-----------------+-------------------+------------------+-----------------------------------+
        || +Filter               |              0 |    1 |       5 |               0 |                 0 | anon[37], a, e   | a:From; a.name == {  AUTOSTRING0} |
        || |                     +----------------+------+---------+-----------------+-------------------+------------------+-----------------------------------+
        || +VarLengthExpand(All) |              0 |    4 |       8 |               0 |                 0 | anon[37], a -- e | (e)<-[:*..4]-(a)                  |
        || |                     +----------------+------+---------+-----------------+-------------------+------------------+-----------------------------------+
        || +Filter               |              0 |    1 |       1 |               0 |                 0 | e                | e.name == {  AUTOSTRING1}         |
        || |                     +----------------+------+---------+-----------------+-------------------+------------------+-----------------------------------+
        || +NodeByLabelScan      |              1 |    1 |       2 |               0 |                 0 | e                | :To                               |
        |+-----------------------+----------------+------+---------+-----------------+-------------------+------------------+-----------------------------------+
        |""".stripMargin)
  }

  test("Plan should have right relationship direction other direction") {
    setUp("To")
    val query = "PROFILE MATCH (a:From {name:'Keanu Reeves'})-[*..4]->(e:To {name:'Andres'}) RETURN *"
    val result = executeWithCostPlannerAndInterpretedRuntimeOnly(query)
    result should havePlanLike(
      """
        |+-----------------------+----------------+------+---------+-----------------+-------------------+------------------+---------------------------------+
        || Operator              | Estimated Rows | Rows | DB Hits | Page Cache Hits | Page Cache Misses | Variables        | Other                           |
        |+-----------------------+----------------+------+---------+-----------------+-------------------+------------------+---------------------------------+
        || +ProduceResults       |              0 |    1 |       0 |               0 |                 0 | anon[37], a, e   |                                 |
        || |                     +----------------+------+---------+-----------------+-------------------+------------------+---------------------------------+
        || +Filter               |              0 |    1 |       5 |               0 |                 0 | anon[37], a, e   | e:To; e.name == {  AUTOSTRING1} |
        || |                     +----------------+------+---------+-----------------+-------------------+------------------+---------------------------------+
        || +VarLengthExpand(All) |              0 |    4 |       8 |               0 |                 0 | anon[37], e -- a | (a)-[:*..4]->(e)                |
        || |                     +----------------+------+---------+-----------------+-------------------+------------------+---------------------------------+
        || +Filter               |              0 |    1 |       1 |               0 |                 0 | a                | a.name == {  AUTOSTRING0}       |
        || |                     +----------------+------+---------+-----------------+-------------------+------------------+---------------------------------+
        || +NodeByLabelScan      |              1 |    1 |       2 |               0 |                 0 | a                | :From                           |
        |+-----------------------+----------------+------+---------+-----------------+-------------------+------------------+---------------------------------+
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
