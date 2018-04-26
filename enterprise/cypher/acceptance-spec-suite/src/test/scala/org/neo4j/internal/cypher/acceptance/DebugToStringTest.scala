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

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.WindowsStringSafe


class DebugToStringTest extends ExecutionEngineFunSuite {

  implicit val windowsSafe = WindowsStringSafe

  /**
    * This tests an internal feature that is not supported or critical for end users. Still nice to see that it works
    * and what the expected outputs are.
    */
  test("cost reporting") {
    val stringResult = graph.execute("CYPHER debug=dumpCosts MATCH (a:A) RETURN *").resultAsString()
    stringResult should equal("""+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
                                || # | planText                                                             | planCost                                                                  | cost              | est cardinality | winner |
                                |+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
                                || 1 | ""                                                                   | ""                                                                        | 1.0               | 1.0             | "WON"  |
                                || 1 | "NodeByLabelScan(a, LabelName(A), Set()) {}"                         | "NodeByLabelScan costs Cost(1.0) cardinality Cardinality(1.0)"            | <null>            | <null>          | <null> |
                                || 1 | ""                                                                   | ""                                                                        | 2.2               | 1.0             | "LOST" |
                                || 1 | "Selection(ListBuffer(HasLabels(Variable(a),List(LabelName(A))))) {" | "Selection costs Cost(2.2) cardinality Cardinality(1.0)"                  | <null>            | <null>          | <null> |
                                || 1 | "  LHS -> AllNodesScan(a, Set()) {}"                                 | "  AllNodesScan costs Cost(1.2) cardinality Cardinality(1.0)"             | <null>            | <null>          | <null> |
                                || 0 | ""                                                                   | ""                                                                        | 2.2               | 1.0             | "WON"  |
                                || 0 | "Selection(ListBuffer(HasLabels(Variable(a),List(LabelName(A))))) {" | "Selection costs Cost(2.2) cardinality Cardinality(1.0)"                  | <null>            | <null>          | <null> |
                                || 0 | "  LHS -> AllNodesScan(a, Set()) {}"                                 | "  AllNodesScan costs Cost(1.2) cardinality Cardinality(1.0)"             | <null>            | <null>          | <null> |
                                || 0 | ""                                                                   | ""                                                                        | 7.700000000000001 | 1.0             | "LOST" |
                                || 0 | "NodeHashJoin(Set(a)) {"                                             | "NodeHashJoin costs Cost(7.700000000000001) cardinality Cardinality(1.0)" | <null>            | <null>          | <null> |
                                || 0 | "  LHS -> AllNodesScan(a, Set()) {}"                                 | "  AllNodesScan costs Cost(1.2) cardinality Cardinality(1.0)"             | <null>            | <null>          | <null> |
                                || 0 | "  RHS -> NodeByLabelScan(a, LabelName(A), Set()) {}"                | "  NodeByLabelScan costs Cost(1.0) cardinality Cardinality(1.0)"          | <null>            | <null>          | <null> |
                                |+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
                                |12 rows
                                |""".stripMargin)
    graph.execute("CYPHER debug=dumpCosts MATCH (a:A:B:C)-[:T]->(b:A:B:C) RETURN *").stream().count()
  }
}
