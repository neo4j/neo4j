/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_5.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.v3_5.expressions.Ands
import org.neo4j.cypher.internal.v3_5.expressions.HasLabels
import org.neo4j.cypher.internal.v3_5.logical.plans.Argument
import org.neo4j.cypher.internal.v3_5.logical.plans.FieldSignature
import org.neo4j.cypher.internal.v3_5.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.v3_5.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.v3_5.logical.plans.ProcedureCall
import org.neo4j.cypher.internal.v3_5.logical.plans.ProcedureReadOnlyAccess
import org.neo4j.cypher.internal.v3_5.logical.plans.ProcedureSignature
import org.neo4j.cypher.internal.v3_5.logical.plans.QualifiedName
import org.neo4j.cypher.internal.v3_5.logical.plans.Selection
import org.neo4j.cypher.internal.v3_5.util.symbols.CTNode
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite
import org.scalatest.Inside.inside

class SelectHasLabelWithJoinTest extends CypherFunSuite with LogicalPlanningTestSupport2 {
  test("should solve labels with joins") {

    val plan = new given {
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

  test("should not solve has-labels check on procedure result with joins") {
    val signature = ProcedureSignature(
      QualifiedName(Seq.empty, "getNode"),
      IndexedSeq.empty,
      Some(IndexedSeq(FieldSignature("node", CTNode))),
      None,
      ProcedureReadOnlyAccess(Array.empty))

    val plan = new given {
      procedure(signature)
      cost = {
        case (_: Selection, _, _) => 1000.0
        case (_: NodeHashJoin, _, _) => 20.0
        case (_: NodeByLabelScan, _, _) => 20.0
      }
    } getLogicalPlanFor "CALL getNode() YIELD node WHERE node:Label RETURN node"

    inside(plan._2) {
      case Selection(Ands(exprs), ProcedureCall(Argument(_), _)) =>
        exprs.toList should matchPattern {
          case List(HasLabels(_, _)) => ()
        }
    }
  }
}
