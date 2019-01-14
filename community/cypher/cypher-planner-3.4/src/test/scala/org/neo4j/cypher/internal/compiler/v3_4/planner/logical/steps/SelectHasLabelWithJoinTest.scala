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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_4.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.v3_4.logical.plans.{NodeByLabelScan, NodeHashJoin, Selection}
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite

class SelectHasLabelWithJoinTest extends CypherFunSuite with LogicalPlanningTestSupport2 {
  test("should solve labels with joins") {

    implicit val plan = new given {
      cost = {
        case (_: Selection, _, _) => 1000.0
        case (_: NodeHashJoin, _, _) => 20.0
        case (_: NodeByLabelScan, _, _) => 20.0
      }
    } getLogicalPlanFor "MATCH (n:Foo:Bar:Baz) RETURN n"

    plan._2 match {
      case NodeHashJoin(_,
      NodeHashJoin(_,
      NodeByLabelScan(_, _, _),
      NodeByLabelScan(_, _, _)),
      NodeByLabelScan(_, _, _)) => ()
      case _ => fail("Not what we expected!")
    }
  }
}
