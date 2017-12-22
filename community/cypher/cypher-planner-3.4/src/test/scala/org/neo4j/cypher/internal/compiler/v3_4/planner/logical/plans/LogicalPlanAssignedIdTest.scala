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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical.plans

import org.neo4j.cypher.internal.compiler.v3_4.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.ir.v3_4.IdName
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.v3_4.{CypherException, Rewriter, topDown}
import org.neo4j.cypher.internal.v3_4.logical.plans.{NestedPlanExpression, _}

class LogicalPlanAssignedIdTest extends CypherFunSuite with LogicalPlanningTestSupport2 {
  test("id survives rewriting") {
    val sr1 = Argument()(solved)
    val pr = Projection(sr1, Map.empty)(solved)

    val prPrim = pr.endoRewrite(topDown(Rewriter.lift {
      case sr: Argument => Argument()(solved)
    }))

    pr.id should equal(prPrim.id)
    pr.lhs.get.id should equal(prPrim.lhs.get.id)
  }

  test("ids are different between plans") {
    val sr1 = Argument()(solved)
    val sr2 = Argument()(solved)
    val apply = Apply(sr1, sr2)(solved)

    sr1.id shouldNot equal(sr2.id)
  }

  test("tree structure is assigned ids in a predictable way") {
    /*
                ApplyAll(6)
        ApplyA(5)         ApplyB(2)
    SR1A(4)  SR2A(3)   SR1B(1) SR2B(0)
     */
    val sr1A = Argument()(solved)
    val sr2A = Argument()(solved)
    val applyA = Apply(sr1A, sr2A)(solved)
    val sr1B = Argument()(solved)
    val sr2B = Argument()(solved)
    val applyB = Apply(sr1B, sr2B)(solved)
    val applyAll = Apply(applyA, applyB)(solved) // I heard you guys like Apply, so we applied an Apply in your Apply

    applyAll.id.x should equal(0)
    applyA.id.x should equal(1)
    sr1A.id.x should equal(2)
    sr2A.id.x should equal(3)
    applyB.id.x should equal(4)
    sr1B.id.x should equal(5)
    sr2B.id.x should equal(6)
  }
}
