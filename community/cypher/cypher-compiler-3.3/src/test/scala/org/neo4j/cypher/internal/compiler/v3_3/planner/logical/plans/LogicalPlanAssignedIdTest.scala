/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans

import org.neo4j.cypher.internal.compiler.v3_3.ast.NestedPlanExpression
import org.neo4j.cypher.internal.compiler.v3_3.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_3.{CypherException, Rewriter, topDown}
import org.neo4j.cypher.internal.ir.v3_3.IdName
import org.neo4j.cypher.internal.v3_3.logical.plans._

class LogicalPlanAssignedIdTest extends CypherFunSuite with LogicalPlanningTestSupport2 {
  test("assignedId survives rewriting") {
    val sr1 = SingleRow()(solved)
    val pr = Projection(sr1, Map.empty)(solved)
    pr.assignIds()

    val prPrim = pr.endoRewrite(topDown(Rewriter.lift {
      case sr: SingleRow => SingleRow()(solved)
    }))

    pr.assignedId should equal(prPrim.assignedId)
    pr.lhs.get.assignedId should equal(prPrim.lhs.get.assignedId)
  }

  test("assignedIds are different between plans") {
    val sr1 = SingleRow()(solved)
    val sr2 = SingleRow()(solved)
    val apply = Apply(sr1, sr2)(solved)
    apply.assignIds()

    sr1.assignedId shouldNot equal(sr2.assignedId)
  }

  test("tree structure is assigned ids in a predictable way") {
    /*
                ApplyAll(6)
        ApplyA(5)         ApplyB(2)
    SR1A(4)  SR2A(3)   SR1B(1) SR2B(0)
     */
    val sr1A = SingleRow()(solved)
    val sr2A = SingleRow()(solved)
    val applyA = Apply(sr1A, sr2A)(solved)
    val sr1B = SingleRow()(solved)
    val sr2B = SingleRow()(solved)
    val applyB = Apply(sr1B, sr2B)(solved)
    val applyAll = Apply(applyA, applyB)(solved) // I heard you guys like Apply, so we applied an Apply in your Apply

    applyAll.assignIds()

    applyAll.assignedId.underlying should equal(0)
    applyA.assignedId.underlying should equal(1)
    sr1A.assignedId.underlying should equal(2)
    sr2A.assignedId.underlying should equal(3)
    applyB.assignedId.underlying should equal(4)
    sr1B.assignedId.underlying should equal(5)
    sr2B.assignedId.underlying should equal(6)
  }

  test("cant assign ids twice") {
    val sr1 = SingleRow()(solved)
    val pr = Projection(sr1, Map.empty)(solved)
    pr.assignIds()
    intercept[CypherException](pr.assignIds())
  }

  test("can assign inside expressions as well") {
    val singleRow = SingleRow()(solved)
    val inner = AllNodesScan(IdName("x"), Set.empty)(solved)
    val filter = Selection(Seq(NestedPlanExpression(inner, literalInt(42))(pos)), singleRow)(solved)

    filter.assignIds()

    val x = inner.assignedId // should not fail
  }

}
