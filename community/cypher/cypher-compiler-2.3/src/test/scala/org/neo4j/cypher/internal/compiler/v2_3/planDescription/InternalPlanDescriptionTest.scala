/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.planDescription

import org.neo4j.cypher.internal.compiler.v2_3.pipes.Pipe
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class InternalPlanDescriptionTest extends CypherFunSuite {

  def pipe: Pipe = mock[Pipe]

  test("flatten behaves like expected for plan with two children") {
    val child1 = PlanDescriptionImpl(pipe.id, "child1", NoChildren, Seq.empty, Set())
    val child2 = PlanDescriptionImpl(pipe.id, "child2", NoChildren, Seq.empty, Set())
    val top = PlanDescriptionImpl(pipe.id, "top", TwoChildren(child1, child2), Seq.empty, Set())

    top.flatten should equal(Seq(top, child1, child2))
  }

  test("single plan flattened stays single") {
    val single = PlanDescriptionImpl(pipe.id, "single", NoChildren, Seq.empty, Set())

    single.flatten should equal(Seq(single))
  }

  test("left leaning plan should also flatten out nicely") {
    val leaf = PlanDescriptionImpl(pipe.id, "leaf", NoChildren, Seq.empty, Set())
    val lvl1 = PlanDescriptionImpl(pipe.id, "lvl1", SingleChild(leaf), Seq.empty, Set())
    val lvl2 = PlanDescriptionImpl(pipe.id, "lvl2", SingleChild(lvl1), Seq.empty, Set())
    val root = PlanDescriptionImpl(pipe.id, "root", SingleChild(lvl2), Seq.empty, Set())

    root.flatten should equal(Seq(root, lvl2, lvl1, leaf))
  }

  test("bushy tree flattens correctly") {
    /*
                  A
             B1      B2
           C1  C2  C3  C4
     */
    val C4 = PlanDescriptionImpl(pipe.id, "C4", NoChildren, Seq.empty, Set())
    val C3 = PlanDescriptionImpl(pipe.id, "C3", NoChildren, Seq.empty, Set())
    val C2 = PlanDescriptionImpl(pipe.id, "C2", NoChildren, Seq.empty, Set())
    val C1 = PlanDescriptionImpl(pipe.id, "C1", NoChildren, Seq.empty, Set())
    val B2 = PlanDescriptionImpl(pipe.id, "B2", TwoChildren(C3, C4), Seq.empty, Set())
    val B1 = PlanDescriptionImpl(pipe.id, "B1", TwoChildren(C1, C2), Seq.empty, Set())
    val A  = PlanDescriptionImpl(pipe.id, "A" , TwoChildren(B1, B2), Seq.empty, Set())

    A.flatten should equal(Seq(A, B1, C1, C2, B2, C3, C4))
  }
}
