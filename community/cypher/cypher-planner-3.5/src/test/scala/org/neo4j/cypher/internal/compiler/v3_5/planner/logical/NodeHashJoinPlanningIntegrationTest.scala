/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical

import org.opencypher.v9_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v3_5.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.ir.v3_5.RegularPlannerQuery
import org.opencypher.v9_0.expressions._
import org.neo4j.cypher.internal.v3_5.logical.plans._

class NodeHashJoinPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should build plans containing joins") {
    val result= (new given {
      cardinality = mapCardinality {
        // node label scan
        case RegularPlannerQuery(queryGraph, _, _) if queryGraph.patternNodes.size == 1 && queryGraph.selections.predicates.size == 1 => 100.0
        // all node scan
        case RegularPlannerQuery(queryGraph, _, _) if queryGraph.patternNodes.size == 1 && queryGraph.selections.predicates.isEmpty => 10000.0
        // expand
        case RegularPlannerQuery(queryGraph, _, _) if queryGraph.patternRelationships.size == 1 => 100.0
        case _                             => Double.MaxValue
      }

    } getLogicalPlanFor "MATCH (a:X)<-[r1]-(b)-[r2]->(c:X) RETURN b")._2

    val expected =
      Selection(
        Seq(Not(Equals(Variable("r1")_, Variable("r2")_)_)_),
        NodeHashJoin(
          Set("b"),
          Expand(
            NodeByLabelScan("a", lblName("X"), Set.empty),
            "a", SemanticDirection.INCOMING, Seq.empty, "b", "r1"),
          Expand(
            NodeByLabelScan("c", lblName("X"), Set.empty),
            "c", SemanticDirection.INCOMING, Seq.empty, "b", "r2")
        )
      )

    result should equal(expected)
  }

  test("should plan hash join when join hint is used") {
    val cypherQuery = """
                        |MATCH (a:A)-[r1:X]->(b)-[r2:X]->(c:C)
                        |USING JOIN ON b
                        |WHERE a.prop = c.prop
                        |RETURN b""".stripMargin

    val result = (new given {
      cardinality = mapCardinality {
        // node label scan
        case RegularPlannerQuery(queryGraph, _, _) if queryGraph.patternNodes.size == 1 && queryGraph.selections.predicates.size == 1 => 100.0
        // all node scan
        case RegularPlannerQuery(queryGraph, _, _) if queryGraph.patternNodes.size == 1 && queryGraph.selections.predicates.isEmpty => 10000.0
        case _ => Double.MaxValue
      }
    }  getLogicalPlanFor cypherQuery)._2

    val expected =
      Selection(
        Seq(
          Not(Equals(Variable("r1") _, Variable("r2") _) _) _,
          Equals(Property(Variable("a") _, PropertyKeyName("prop") _) _, Property(Variable("c") _, PropertyKeyName("prop") _) _) _
        ),
        NodeHashJoin(
          Set("b"),
          Expand(
            NodeByLabelScan("a", lblName("A"), Set.empty),
            "a", SemanticDirection.OUTGOING, Seq(RelTypeName("X") _), "b", "r1", ExpandAll),
          Expand(
            NodeByLabelScan("c", lblName("C"), Set.empty),
            "c", SemanticDirection.INCOMING, Seq(RelTypeName("X") _), "b", "r2", ExpandAll)
        )
      )

    result shouldEqual expected
  }
}
