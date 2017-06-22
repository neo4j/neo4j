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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime

import org.neo4j.cypher.internal.compiler.v3_3.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v3_3.SemanticDirection
import org.neo4j.cypher.internal.frontend.v3_3.ast.{LabelName, True}
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.ir.v3_3.IdName

class RegisterAllocationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {
  test("only single allnodes scan") {
    // given
    val plan = AllNodesScan(IdName("x"), Set.empty)(solved)

    // when
    val allocations = RegisterAllocation.allocateRegisters(plan)

    // then
    allocations should have size 1
    allocations(plan) should equal(PipelineInformation(Map("x" -> LongSlot(0)), 1, 0))
  }

  test("single labelscan scan") {
    // given
    val plan = NodeByLabelScan(IdName("x"), LabelName("label")(pos), Set.empty)(solved)

    // when
    val allocations = RegisterAllocation.allocateRegisters(plan)

    // then
    allocations should have size 1
    allocations(plan) should equal(PipelineInformation(Map("x" -> LongSlot(0)), 1, 0))
  }

  test("labelscan with filtering") {
    // given
    val leaf = NodeByLabelScan(IdName("x"), LabelName("label")(pos), Set.empty)(solved)
    val filter = Selection(Seq(True()(pos)), leaf)(solved)

    // when
    val allocations = RegisterAllocation.allocateRegisters(filter)

    // then
    allocations should have size 2
    allocations(leaf) should equal(PipelineInformation(Map("x" -> LongSlot(0)), 1, 0))
    allocations(filter) shouldBe theSameInstanceAs (allocations(leaf))
  }

  test("single node with expand") {
    // given
    val allNodesScan = AllNodesScan(IdName("x"), Set.empty)(solved)
    val expand = Expand(allNodesScan, IdName("x"), SemanticDirection.INCOMING, Seq.empty, IdName("z"), IdName("r"), ExpandAll)(solved)

    // when
    val allocations = RegisterAllocation.allocateRegisters(expand)

    // then we'll end up with two pipelines
    allocations should have size 2
    val labelScanAllocations = allocations(allNodesScan)
    labelScanAllocations should equal(
      PipelineInformation(Map("x" -> LongSlot(0)), numberOfLongs = 1, numberOfReferences = 0))

    val expandAllocations = allocations(expand)
    expandAllocations should equal(
      PipelineInformation(Map("x" -> LongSlot(0), "r" -> LongSlot(1), "z" -> LongSlot(2)), numberOfLongs = 3, numberOfReferences = 0))
  }

  test("single node with expand into") {
    // given
    val allNodesScan = AllNodesScan(IdName("x"), Set.empty)(solved)
    val expand = Expand(allNodesScan, IdName("x"), SemanticDirection.INCOMING, Seq.empty, IdName("x"), IdName("r"), ExpandInto)(solved)

    // when
    val allocations = RegisterAllocation.allocateRegisters(expand)

    // then we'll end up with two pipelines
    allocations should have size 2
    val labelScanAllocations = allocations(allNodesScan)
    labelScanAllocations should equal(
      PipelineInformation(Map("x" -> LongSlot(0)), numberOfLongs = 1, numberOfReferences = 0))

    val expandAllocations = allocations(expand)
    expandAllocations should equal(
      PipelineInformation(Map("x" -> LongSlot(0), "r" -> LongSlot(1)), numberOfLongs = 2, numberOfReferences = 0))
  }

//  test("single node with two expands") {
//    // given
//    val root = AllNodesScan(IdName("x"), Set.empty)(solved)
//    val e1 = Expand(root, IdName("x"), SemanticDirection.INCOMING, Seq.empty, IdName("z"), IdName("r1"), ExpandAll)(solved)
//    val e2 = Expand(e1, IdName("z"), SemanticDirection.INCOMING, Seq.empty, IdName("y"), IdName("r2"), ExpandAll)(solved)
//
//    // when
//    val allocations = RegisterAllocation.allocateRegisters(e2)
//
//    // then
//    allocations should have size 3
//    val allocations1 = allocations(e2)
//    allocations1 should equal(
//      PipelineInformation(Map("x" -> LongSlot(0), "r1" -> LongSlot(1), "z" -> LongSlot(2), "r2" -> LongSlot(3), "y" -> LongSlot(4)), 5, 0))
//
//    allocations1 should be theSameInstanceAs allocations(e1)
//    allocations1 should be theSameInstanceAs allocations(root)
//  }
//
//  test("single expandInto") {
//    // given
//    val root = AllNodesScan(IdName("x"), Set.empty)(solved)
//    val plan = Expand(root, IdName("x"), SemanticDirection.INCOMING, Seq.empty, IdName("x"), IdName("r1"), ExpandInto)(solved)
//
//    // when
//    val allocations = RegisterAllocation.allocateRegisters(plan)
//
//    // then
//    allocations should have size 2
//    allocations(plan) should equal(
//      PipelineInformation(Map("x" -> LongSlot(0), "r1" -> LongSlot(1)), 2, 0))
//  }


}
