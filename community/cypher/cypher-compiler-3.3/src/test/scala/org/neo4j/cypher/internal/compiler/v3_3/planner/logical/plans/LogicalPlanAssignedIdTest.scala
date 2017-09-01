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

import org.neo4j.cypher.internal.compiler.v3_3.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.frontend.v3_3.{Rewriter, topDown}
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite

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
         6
      2     5
    0  1   3 4
     */
    val sr1A = SingleRow()(solved)
    val sr2A = SingleRow()(solved)
    val applyA = Apply(sr1A, sr2A)(solved)
    val sr1B = SingleRow()(solved)
    val sr2B = SingleRow()(solved)
    val applyB = Apply(sr1B, sr2B)(solved)
    val applyAll = Apply(applyA, applyB)(solved) // I heard you guys like Apply, so we applied an Apply in your Apply

    applyAll.assignIds()

    sr1A.assignedId should equal(0)
    sr2A.assignedId should equal(1)
    applyA.assignedId should equal(2)
    sr1B.assignedId should equal(3)
    sr2B.assignedId should equal(4)
    applyB.assignedId should equal(5)
    applyAll.assignedId should equal(6)
  }

}
