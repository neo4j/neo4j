/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2.codegen

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.collection.primitive.PrimitiveLongIterator
import org.neo4j.cypher.internal.compiler.v3_2.executionplan.ExecutionPlanBuilder.tracer
import org.neo4j.cypher.internal.compiler.v3_2.executionplan.InternalExecutionResult
import org.neo4j.cypher.internal.compiler.v3_2.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v3_2.spi.{InternalResultRow, InternalResultVisitor, QueryContext}
import org.neo4j.cypher.internal.compiler.v3_2.{CostBasedPlannerName, NormalMode, TaskCloser}
import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.symbols._
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_2.{ParameterNotFoundException, SemanticDirection, SemanticTable, _}
import org.neo4j.cypher.internal.ir.v3_2.IdName
import org.neo4j.cypher.internal.spi.v3_2.TransactionalContextWrapper
import org.neo4j.cypher.internal.spi.v3_2.codegen.GeneratedQueryStructure
import org.neo4j.graphdb.{Direction, Node, Relationship}
import org.neo4j.kernel.api.ReadOperations
import org.neo4j.kernel.impl.api.RelationshipVisitor
import org.neo4j.kernel.impl.api.store.RelationshipIterator
import org.neo4j.kernel.impl.core.{NodeManager, NodeProxy, RelationshipProxy}

import scala.collection.{JavaConverters, mutable}

class CodeGeneratorTest extends CypherFunSuite with LogicalPlanningTestSupport {

  private val generator = new CodeGenerator(GeneratedQueryStructure, CodeGenConfiguration(mode = ByteCodeMode))

  test("all nodes scan") { // MATCH a RETURN a
    //given
    val plan = ProduceResult(List("a"), AllNodesScan(IdName("a"), Set.empty)(solved))

    //when
    val compiled = compileAndExecute(plan)

    //then
    val result = getNodesFromResult(compiled, "a")
    result should equal(List(
      Map("a" -> aNode),
      Map("a" -> bNode),
      Map("a" -> cNode),
      Map("a" -> dNode),
      Map("a" -> eNode),
      Map("a" -> fNode),
      Map("a" -> gNode),
      Map("a" -> hNode),
      Map("a" -> iNode)))
  }

  test("label scan") {// MATCH (a:T1) RETURN a
    //given
    val plan = ProduceResult(List("a"), NodeByLabelScan(IdName("a"), lblName("T1"), Set.empty)(solved))

    //when
    val compiled = compileAndExecute(plan)

    //then
    val result = getNodesFromResult(compiled, "a")
    result should equal(List(
      Map("a" -> aNode),
      Map("a" -> bNode),
      Map("a" -> cNode)
    ))
  }

  test("hash join of all nodes scans") { // MATCH a RETURN a
    //given
    val lhs = AllNodesScan(IdName("a"), Set.empty)(solved)
    val rhs = AllNodesScan(IdName("a"), Set.empty)(solved)
    val join = NodeHashJoin(Set(IdName("a")), lhs, rhs)(solved)
    val plan = ProduceResult(List("a"), join)

    //when
    val compiled = compileAndExecute(plan)

    //then
    val result = getNodesFromResult(compiled, "a")
    result should equal(List(
      Map("a" -> aNode),
      Map("a" -> bNode),
      Map("a" -> cNode),
      Map("a" -> dNode),
      Map("a" -> eNode),
      Map("a" -> fNode),
      Map("a" -> gNode),
      Map("a" -> hNode),
      Map("a" -> iNode)))
  }

  test("hash join on multiple keys") {
    //given
    val lhs = Expand(AllNodesScan(IdName("a"), Set.empty)(solved), IdName("a"), SemanticDirection.OUTGOING, Seq.empty, IdName("b"), IdName("r1"), ExpandAll)(solved)
    val rhs = Expand(AllNodesScan(IdName("a"), Set.empty)(solved), IdName("a"), SemanticDirection.OUTGOING, Seq.empty, IdName("b"), IdName("r1"), ExpandAll)(solved)
    val join = NodeHashJoin(Set(IdName("a"), IdName("b")), lhs, rhs)(solved)
    val plan = ProduceResult(List("a"), join)

    //when
    val compiled = compileAndExecute(plan)

    //then
    val result = getNodesFromResult(compiled, "a")
    result should equal(List(
      Map("a" -> aNode),
      Map("a" -> bNode),
      Map("a" -> cNode),
      Map("a" -> fNode),
      Map("a" -> gNode),
      Map("a" -> hNode),
      Map("a" -> iNode)
    ))
  }

  test("cartesian product of two label scans") {
    //given
    val lhs = NodeByLabelScan(IdName("a"), lblName("T1"), Set.empty)(solved)
    val rhs = NodeByLabelScan(IdName("b"), lblName("T2"), Set.empty)(solved)
    val join = CartesianProduct(lhs, rhs)(solved)
    val plan = ProduceResult(List("a", "b"), join)

    //when
    val compiled = compileAndExecute(plan)

    //then
    val result = getNodesFromResult(compiled, "a", "b")
    result should equal(List(
      Map("a" -> aNode, "b" -> fNode),
      Map("a" -> aNode, "b" -> gNode),
      Map("a" -> bNode, "b" -> fNode),
      Map("a" -> bNode, "b" -> gNode),
      Map("a" -> cNode, "b" -> fNode),
      Map("a" -> cNode, "b" -> gNode)))
  }

  test("all nodes scan + expand") { // MATCH (a)-[r]->(b) RETURN a, b
    //given
    val plan = ProduceResult(List("a", "b"),
        Expand(
          AllNodesScan(IdName("a"), Set.empty)(solved), IdName("a"),
          SemanticDirection.OUTGOING, Seq.empty, IdName("b"), IdName("r"), ExpandAll)(solved))

    //when
    val compiled = compileAndExecute(plan)

    //then
    val result = getNodesFromResult(compiled, "a", "b")

    result should equal(List(
      Map("a" -> aNode, "b" -> dNode),
      Map("a" -> bNode, "b" -> dNode),
      Map("a" -> cNode, "b" -> eNode),
      Map("a" -> fNode, "b" -> dNode),
      Map("a" -> gNode, "b" -> eNode),
      Map("a" -> hNode, "b" -> iNode),
      Map("a" -> iNode, "b" -> hNode)))
  }

  test("label scan + expand outgoing") { // MATCH (a:T1)-[r]->(b) RETURN a, b
    //given
    val plan = ProduceResult(List("a", "b"),
        Expand(
          NodeByLabelScan(IdName("a"), lblName("T1"), Set.empty)(solved), IdName("a"),
          SemanticDirection.OUTGOING, Seq.empty, IdName("b"), IdName("r"), ExpandAll)(solved))

    //when
    val compiled = compileAndExecute(plan)

    //then
    val result = getNodesFromResult(compiled, "a", "b")

    result should equal(List(
      Map("a" -> aNode, "b" -> dNode),
      Map("a" -> bNode, "b" -> dNode),
      Map("a" -> cNode, "b" -> eNode)
    ))
  }

  test("all node scan+ expand outgoing with one type") { // MATCH (a)-[r:R1]->(b) RETURN a, b
  //given
  val plan = ProduceResult(List("a", "b"),
        Expand(
          AllNodesScan(IdName("a"), Set.empty)(solved), IdName("a"), SemanticDirection.OUTGOING, Seq(RelTypeName("R1")(null)), IdName("b"), IdName("r"), ExpandAll)(solved))

    //when
    val compiled = compileAndExecute(plan)

    //then
    val result = getNodesFromResult(compiled, "a", "b")

    result should equal(List(
      Map("a" -> aNode, "b" -> dNode),
      Map("a" -> bNode, "b" -> dNode),
      Map("a" -> cNode, "b" -> eNode)
    ))
  }

  test("all node scan+ expand outgoing with multiple types") { // MATCH (a)-[r:R1|R2]->(b) RETURN a, b
  //given
  val plan = ProduceResult(List("a", "b"),
        Expand(
          AllNodesScan(IdName("a"), Set.empty)(solved), IdName("a"), SemanticDirection.OUTGOING,
          Seq(RelTypeName("R1")(pos), RelTypeName("R2")(pos)), IdName("b"), IdName("r"), ExpandAll)(solved))

    //when
    val compiled = compileAndExecute(plan)

    //then
    val result = getNodesFromResult(compiled, "a", "b")

    result should equal(List(
      Map("a" -> aNode, "b" -> dNode),
      Map("a" -> bNode, "b" -> dNode),
      Map("a" -> cNode, "b" -> eNode),
      Map("a" -> fNode, "b" -> dNode),
      Map("a" -> gNode, "b" -> eNode)))
  }

  test("label scan + expand incoming") { // // MATCH (a:T1)<-[r]-(b) RETURN a, b
  //given
  val plan = ProduceResult(List("a", "b"),
                Expand(
                  NodeByLabelScan(IdName("a"), lblName("T1"), Set.empty)(solved), IdName("a"),
                  SemanticDirection.INCOMING, Seq.empty, IdName("b"), IdName("r"), ExpandAll)(solved))

    //when
    val compiled = compileAndExecute(plan)

    //then
    val result = getNodesFromResult(compiled, "a", "b")

    result shouldBe empty
  }

  test("label scan + optional expand incoming") {
  //given
  val plan = ProduceResult(List("a", "b"),
                             OptionalExpand(
                               NodeByLabelScan(IdName("a"), lblName("T1"), Set.empty)(solved), IdName("a"),
                               SemanticDirection.OUTGOING, Seq.empty, IdName("b"), IdName("r"), ExpandAll,
                               Seq(HasLabels(varFor("b"), Seq(LabelName("T1")(pos)))(pos)))(solved))

    //when
    val compiled: InternalExecutionResult = compileAndExecute(plan)
    //then
    val result = getNodesFromResult(compiled, "a", "b")

    result should equal(List(
      Map("a" -> aNode, "b" -> null),
      Map("a" -> bNode, "b" -> null),
      Map("a" -> cNode, "b" -> null)
      ))
  }

  test("label scan + expand both directions") { // MATCH (a:T1)-[r]-(b) RETURN a, b
  //given
  val plan = ProduceResult(List("a", "b"),
      Expand(
        NodeByLabelScan(IdName("a"), lblName("T1"), Set.empty)(solved), IdName("a"), SemanticDirection.BOTH,
        Seq.empty, IdName("b"), IdName("r"), ExpandAll)(solved))

    //when
    val compiled = compileAndExecute(plan)

    //then
    val result = getNodesFromResult(compiled, "a", "b")

    result should equal(List(
      Map("a" -> aNode, "b" -> dNode),
      Map("a" -> bNode, "b" -> dNode),
      Map("a" -> cNode, "b" -> eNode)
    ))
  }

  test("expand into self loop") {
    //given
    val scanT1 = NodeByLabelScan(IdName("a"), lblName("T1"), Set.empty)(solved)
    val expandInto = Expand(
      scanT1, IdName("a"), SemanticDirection.INCOMING,
      Seq.empty, IdName("a"), IdName("r2"), ExpandInto)(solved)

    val plan = ProduceResult(List("a"), expandInto)

    //when
    val compiled = compileAndExecute(plan)

    //then
    val result = getNodesFromResult(compiled, "a")

    result shouldBe empty
  }

  test("expand into on top of expand all") {
    //given
    val scanT1 = NodeByLabelScan(IdName("a"), lblName("T1"), Set.empty)(solved)
    val expandAll = Expand(
      scanT1, IdName("a"), SemanticDirection.OUTGOING,
      Seq.empty, IdName("b"), IdName("r1"), ExpandAll)(solved)
    val expandInto = Expand(
      expandAll, IdName("b"), SemanticDirection.INCOMING,
      Seq.empty, IdName("a"), IdName("r2"), ExpandInto)(solved)

    val plan = ProduceResult(List("a", "b"), expandInto)

    //when
    val compiled = compileAndExecute(plan)

    //then
    val result = getNodesFromResult(compiled, "a", "b")

    result should equal(List(
      Map("a" -> aNode, "b" -> dNode),
      Map("a" -> bNode, "b" -> dNode),
      Map("a" -> cNode, "b" -> eNode)
    ))
  }

  test("optional expand into self loop") {
    //given
    val scanT1 = NodeByLabelScan(IdName("a"), lblName("T1"), Set.empty)(solved)
    val optionalExpandInto = OptionalExpand(
      scanT1, IdName("a"), SemanticDirection.OUTGOING,
      Seq.empty, IdName("a"), IdName("r2"), ExpandInto, Seq(HasLabels(varFor("a"), Seq(LabelName("T2")(pos)))(pos)))(solved)


    val plan = ProduceResult(List("a", "r2"), optionalExpandInto)

    //when
    val compiled = compileAndExecute(plan)

    //then
    val result = getResult(compiled, "a", "r2")

    result should equal(List(
      Map("a" -> aNode, "r2" -> null),
      Map("a" -> bNode, "r2" -> null),
      Map("a" -> cNode, "r2" -> null)))
  }

  test("optional expand into on top of expand all") {
    //given
    val scanT1 = NodeByLabelScan(IdName("a"), lblName("T1"), Set.empty)(solved)
    val expandAll = Expand(
      scanT1, IdName("a"), SemanticDirection.OUTGOING,
      Seq.empty, IdName("b"), IdName("r1"), ExpandAll)(solved)
    val optionalExpandInto = OptionalExpand(
      expandAll, IdName("a"), SemanticDirection.OUTGOING,
      Seq.empty, IdName("b"), IdName("r2"), ExpandInto, Seq(HasLabels(varFor("b"), Seq(LabelName("T2")(pos)))(pos)))(solved)


    val plan = ProduceResult(List("a", "b", "r2"), optionalExpandInto)

    //when
    val compiled = compileAndExecute(plan)

    //then
    val result = getResult(compiled, "a", "b", "r2")

    result should equal(List(
      Map("a" -> aNode, "b" -> dNode, "r2" -> null),
      Map("a" -> bNode, "b" -> dNode, "r2" -> null),
      Map("a" -> cNode, "b" -> eNode, "r2" -> null)
    ))
  }

  test("expand into on top of expand all with relationship types") {
    //given
    val scanT2 = NodeByLabelScan(IdName("a"), lblName("T2"), Set.empty)(solved)
    val expandAll = Expand(
      scanT2, IdName("a"), SemanticDirection.OUTGOING,
      Seq(RelTypeName("R2")(pos)), IdName("b"), IdName("r1"), ExpandAll)(solved)
    val expandInto = Expand(
      expandAll, IdName("b"), SemanticDirection.INCOMING,
      Seq(RelTypeName("R2")(pos)), IdName("a"), IdName("r2"), ExpandInto)(solved)


    val plan = ProduceResult(List("a", "b"), expandInto)

    //when
    val compiled = compileAndExecute(plan)

    //then
    val result = getNodesFromResult(compiled, "a", "b")

    result should equal(List(
      Map("a" -> fNode, "b" -> dNode),
      Map("a" -> gNode, "b" -> eNode)
    ))
  }

  test("optional expand into on top of expand all with relationship types") {
    //given
    val scanT2 = NodeByLabelScan(IdName("a"), lblName("T2"), Set.empty)(solved)
    val expandAll = Expand(
      scanT2, IdName("a"), SemanticDirection.OUTGOING,
      Seq(RelTypeName("R2")(pos)), IdName("b"), IdName("r1"), ExpandAll)(solved)
    val expandInto = OptionalExpand(
      expandAll, IdName("b"), SemanticDirection.INCOMING,
      Seq(RelTypeName("R2")(pos)), IdName("a"), IdName("r2"), ExpandInto)(solved)


    val plan = ProduceResult(List("a", "b", "r2"), expandInto)

    //when
    val compiled = compileAndExecute(plan)

    //then
    val result = getResult(compiled, "a", "b", "r2")

    result should equal(List(
      Map("a" -> fNode, "b" -> dNode, "r2" -> relMap(14L).relationship),
      Map("a" -> gNode, "b" -> eNode, "r2" -> relMap(15L).relationship)
    ))
  }


  test("expand into on top of expand all with a loop") {
    //given
    val scanT3 = NodeByLabelScan(IdName("a"), lblName("T3"), Set.empty)(solved)
    val expandAll = Expand(
      scanT3, IdName("a"), SemanticDirection.OUTGOING,
      Seq(RelTypeName("R3")(pos)), IdName("b"), IdName("r1"), ExpandAll)(solved)
    val expandInto = Expand(
      expandAll, IdName("b"), SemanticDirection.INCOMING,
      Seq(RelTypeName("R3")(pos)), IdName("a"), IdName("r2"), ExpandInto)(solved)


    val plan = ProduceResult(List("a", "b"), expandInto)

    //when
    val compiled = compileAndExecute(plan)

    //then
    val result = getNodesFromResult(compiled, "a", "b")

    result should equal(List(
      Map("a" -> hNode, "b" -> iNode),
      Map("a" -> iNode, "b" -> hNode)
    ))
  }

  test("optional expand into on top of expand all with a loop") {
    //given
    val scanT3 = NodeByLabelScan(IdName("a"), lblName("T3"), Set.empty)(solved)
    val expandAll = Expand(
      scanT3, IdName("a"), SemanticDirection.OUTGOING,
      Seq(RelTypeName("R3")(pos)), IdName("b"), IdName("r1"), ExpandAll)(solved)
    val expandInto = OptionalExpand(
      expandAll, IdName("b"), SemanticDirection.INCOMING,
      Seq(RelTypeName("R3")(pos)), IdName("a"), IdName("r2"), ExpandInto,
      Seq(HasLabels(varFor("b"), Seq(LabelName("T2")(pos)))(pos)))(solved)


    val plan = ProduceResult(List("a", "b", "r2"), expandInto)

    //when
    val compiled = compileAndExecute(plan)

    //then
    val result = getResult(compiled, "a", "b", "r2")

    result should equal(List(
      Map("a" -> hNode, "b" -> iNode, "r2" -> null),
      Map("a" -> iNode, "b" -> hNode, "r2" -> null)
    ))
  }

  test("hash join on top of two expands from two all node scans") {
    // MATCH (a)-[r1]->(b)<-[r2]-(c) RETURN a,b,c (kind of nothing enforcing that r1 and r2 are distinct)

    //given
    val lhs = Expand(AllNodesScan(IdName("a"), Set.empty)(solved), IdName("a"), SemanticDirection.OUTGOING, Seq.empty, IdName("b"), IdName("r1"), ExpandAll)(solved)
    val rhs = Expand(AllNodesScan(IdName("c"), Set.empty)(solved), IdName("c"), SemanticDirection.OUTGOING, Seq.empty, IdName("b"), IdName("r2"), ExpandAll)(solved)
    val plan = ProduceResult(List("a", "b", "c"), NodeHashJoin(Set(IdName("b")), lhs, rhs)(solved))

    val compiled = compileAndExecute(plan)

    //then
    val result = getNodesFromResult(compiled, "a", "b", "c")

    result.toSet should equal(Set(
      Map("a" -> aNode, "b" -> dNode, "c" -> fNode),
      Map("a" -> aNode, "b" -> dNode, "c" -> bNode),
      Map("a" -> aNode, "b" -> dNode, "c" -> aNode),
      Map("a" -> bNode, "b" -> dNode, "c" -> aNode),
      Map("a" -> bNode, "b" -> dNode, "c" -> bNode),
      Map("a" -> bNode, "b" -> dNode, "c" -> fNode),
      Map("a" -> fNode, "b" -> dNode, "c" -> aNode),
      Map("a" -> fNode, "b" -> dNode, "c" -> bNode),
      Map("a" -> fNode, "b" -> dNode, "c" -> fNode),
      Map("a" -> cNode, "b" -> eNode, "c" -> cNode),
      Map("a" -> cNode, "b" -> eNode, "c" -> gNode),
      Map("a" -> gNode, "b" -> eNode, "c" -> cNode),
      Map("a" -> gNode, "b" -> eNode, "c" -> gNode),
      Map("a" -> hNode, "b" -> iNode, "c" -> hNode),
      Map("a" -> iNode, "b" -> hNode, "c" -> iNode)
    ))
  }

  test("hash join on top of two expands from two label scans") {
    // MATCH (a:T1)-[r1]->(b)<-[r2]-(c:T2) RETURN b

    //given
    val lhs = Expand(NodeByLabelScan(IdName("a"), lblName("T1"), Set.empty)(solved), IdName("a"),
      SemanticDirection.OUTGOING, Seq.empty, IdName("b"), IdName("r1"), ExpandAll)(solved)
    val rhs = Expand(NodeByLabelScan(IdName("c"), lblName("T2"), Set.empty)(solved), IdName("c"),
      SemanticDirection.OUTGOING, Seq.empty, IdName("b"), IdName("r2"), ExpandAll)(solved)
    val join = NodeHashJoin(Set(IdName("b")), lhs, rhs)(solved)
    val plan = ProduceResult(List("a", "b", "c"), join)

    val compiled = compileAndExecute(plan)

    //then
    val result = getNodesFromResult(compiled, "a", "b", "c")

    result.toSet should equal(Set(
      Map("a" -> aNode, "b" -> dNode, "c" -> fNode),
      Map("a" -> bNode, "b" -> dNode, "c" -> fNode),
      Map("a" -> cNode, "b" -> eNode, "c" -> gNode)
    ))
  }

  test("hash join on top of hash join") {

    //given
    val scan1 = NodeByLabelScan(IdName("a"), lblName("T1"), Set.empty)(solved)
    val scan2 = NodeByLabelScan(IdName("a"), lblName("T1"), Set.empty)(solved)
    val scan3 = NodeByLabelScan(IdName("a"), lblName("T1"), Set.empty)(solved)
    val join1 = NodeHashJoin(Set(IdName("a")), scan1, scan2)(solved)
    val join2 = NodeHashJoin(Set(IdName("a")), scan3, join1)(solved)
    val projection = Projection(join2, Map("a" -> varFor("a")))(solved)
    val plan = ProduceResult(List("a"), projection)

    val compiled = compileAndExecute(plan)

    //then
    val result = getNodesFromResult(compiled, "a")

    result.toSet should equal(Set(
      Map("a" -> aNode),
      Map("a" -> bNode),
      Map("a" -> cNode)
    ))
  }

  test("project literal") {
    val plan = ProduceResult(List("a"), Projection(SingleRow()(solved), Map("a" -> SignedDecimalIntegerLiteral("1")(pos)))(solved))
    val compiled = compileAndExecute(plan)

    //then
    val result = getResult(compiled, "a")
    result.toSet should equal(Set(Map("a" -> 1)))
  }

  test("project parameter") {

    val plan = ProduceResult(List("a"), Projection(SingleRow()(solved), Map("a" -> Parameter("FOO", CTAny)(pos)))(solved))
    val compiled = compileAndExecute(plan, Map("FOO" -> "BAR"))

    //then
    val result = getResult(compiled, "a")
    result.toSet should equal(Set(Map("a" -> "BAR")))
  }

  test("project nodes") {
    val scan = AllNodesScan(IdName("a"), Set.empty)(solved)
    val plan = ProduceResult(List("a"), Projection(scan, Map("a" -> varFor("a")))(solved))
    val compiled = compileAndExecute(plan)

    //then
    val result = getNodesFromResult(compiled, "a")
    result should equal(List(
      Map("a" -> aNode),
      Map("a" -> bNode),
      Map("a" -> cNode),
      Map("a" -> dNode),
      Map("a" -> eNode),
      Map("a" -> fNode),
      Map("a" -> gNode),
      Map("a" -> hNode),
      Map("a" -> iNode)))
  }

  test("project relationships") { // MATCH (a)-[r]->(b) WITH r RETURN r
    //given
    val expand = Expand(
        AllNodesScan(IdName("a"), Set.empty)(solved), IdName("a"),
        SemanticDirection.OUTGOING, Seq.empty, IdName("b"), IdName("r"), ExpandAll)(solved)
    val projection = Projection(expand, Map("r" -> varFor("r")))(solved)
    val plan = ProduceResult(List("r"), projection)

    //when
    val compiled = compileAndExecute(plan)

    //then
    val result = getResult(compiled, "r")

    result should equal(List(
      Map("r" -> relMap(11L).relationship),
      Map("r" -> relMap(12L).relationship),
      Map("r" -> relMap(13L).relationship),
      Map("r" -> relMap(14L).relationship),
      Map("r" -> relMap(15L).relationship),
      Map("r" -> relMap(16L).relationship),
      Map("r" -> relMap(17L).relationship)))
  }

  test("project addition of two ints") {
    val lhs = SignedDecimalIntegerLiteral("1")(pos)
    val rhs = SignedDecimalIntegerLiteral("3")(pos)
    val add = Add(lhs, rhs)(pos)

    val plan = ProduceResult(List("a"), Projection(SingleRow()(solved), Map("a" -> add))(solved))
    val compiled = compileAndExecute(plan)

    //then
    val result = getResult(compiled, "a")
    result.toSet should equal(Set(Map("a" -> 4)))
  }

  test("project subtraction of two ints") {
    val lhs = SignedDecimalIntegerLiteral("7")(pos)
    val rhs = SignedDecimalIntegerLiteral("5")(pos)
    val subtract = Subtract(lhs, rhs)(pos)

    val plan = ProduceResult(List("a"), Projection(SingleRow()(solved), Map("a" -> subtract))(solved))
    val compiled = compileAndExecute(plan)

    //then
    val result = getResult(compiled, "a")
    result.toSet should equal(Set(Map("a" -> 2)))
  }

  test("project addition of int and double") {
    val lhs = SignedDecimalIntegerLiteral("1")(pos)
    val rhs = DecimalDoubleLiteral("3.0")(pos)
    val add = Add(lhs, rhs)(pos)

    val plan = ProduceResult(List("a"), Projection(SingleRow()(solved), Map("a" -> add))(solved))
    val compiled = compileAndExecute(plan)

    //then
    val result = getResult(compiled, "a")
    result.toSet should equal(Set(Map("a" -> (1L + 3.0))))
  }

  test("project addition of int and String") {
    val lhs = SignedDecimalIntegerLiteral("1")(pos)
    val rhs = StringLiteral("two")(pos)
    val add = Add(lhs, rhs)(pos)

    val plan = ProduceResult(List("a"), Projection(SingleRow()(solved), Map("a" -> add))(solved))
    val compiled = compileAndExecute(plan)

    //then
    val result = getResult(compiled, "a")
    result.toSet should equal(Set(Map("a" -> "1two")))
  }

  test("project addition of int and value from params") {
    val lhs = SignedDecimalIntegerLiteral("1")(pos)
    val rhs = Parameter("FOO", CTAny)(pos)
    val add = Add(lhs, rhs)(pos)
    val plan = ProduceResult(List("a"), Projection(SingleRow()(solved), Map("a" -> add))(solved))
    val compiled = compileAndExecute(plan, Map("FOO" -> Long.box(3L)))

    //then
    val result = getResult(compiled, "a")
    result.toSet should equal(Set(Map("a" -> 4)))
  }

  test("project addition of two values coming from params") {
    val lhs = Parameter("FOO", CTAny)(pos)
    val rhs = Parameter("BAR", CTAny)(pos)
    val add = Add(lhs, rhs)(pos)
    val plan = ProduceResult(List("a"), Projection(SingleRow()(solved), Map("a" -> add))(solved))
    val compiled = compileAndExecute(plan, Map("FOO" -> Long.box(3L), "BAR" -> Long.box(1L)))

    //then
    val result = getResult(compiled, "a")
    result.toSet should equal(Set(Map("a" -> 4)))
  }

  test("project collection") {
    val collection = ListLiteral(Seq(Parameter("FOO", CTAny)(pos), Parameter("BAR", CTAny)(pos)))(pos)
    val plan = ProduceResult(List("a"), Projection(SingleRow()(solved), Map("a" -> collection))(solved))
    val compiled = compileAndExecute(plan, Map("FOO" -> Long.box(3L), "BAR" -> Long.box(1L)))

    //then
    val result = getResult(compiled, "a")
    result.toSet should equal(Set(Map("a" -> List(3, 1))))
  }

  test("project map") {
    val map = MapExpression(Seq((PropertyKeyName("FOO")(pos), Parameter("BAR", CTAny)(pos))))(pos)
    val plan = ProduceResult(List("a"), Projection(SingleRow()(solved), Map("a" -> map))(solved))
    val compiled = compileAndExecute(plan, Map("FOO" -> Long.box(3L), "BAR" -> Long.box(1L)))

    //then
    val result = getResult(compiled, "a")
    result.toSet should equal(Set(Map("a" -> Map("FOO" -> 1))))
  }

  test("string equality") {
    val equals = Equals(StringLiteral("a string")(pos), StringLiteral("a string")(pos))(pos)
    val plan = ProduceResult(List("result"), Projection(SingleRow()(solved), Map("result" -> equals))(solved))
    val compiled = compileAndExecute(plan)

    //then
    val result = getResult(compiled, "result")
    result.toSet should equal(Set(Map("result" -> true)))
  }

  test("number equality, double and long") {
    val equals = Equals(SignedDecimalIntegerLiteral("9007199254740993")(pos), DecimalDoubleLiteral("9007199254740992")(pos))(pos)
    val plan = ProduceResult(List("result"), Projection(SingleRow()(solved), Map("result" -> equals))(solved))
    val compiled = compileAndExecute(plan)

    //then
    val result = getResult(compiled, "result")
    result.toSet should equal(Set(Map("result" -> false)))
  }

  test("number equality, long and long") {
    val equals = Equals(SignedDecimalIntegerLiteral("9007199254740993")(pos), SignedDecimalIntegerLiteral("9007199254740992")(pos))(pos)
    val plan = ProduceResult(List("result"), Projection(SingleRow()(solved), Map("result" -> equals))(solved))
    val compiled = compileAndExecute(plan)

    //then
    val result = getResult(compiled, "result")
    result.toSet should equal(Set(Map("result" -> false)))
  }

  test("number equality, one from parameter") {
    val equals = Equals(SignedDecimalIntegerLiteral("9007199254740993")(pos), Parameter("BAR", CTAny)(pos))(pos)
    val plan = ProduceResult(List("result"), Projection(SingleRow()(solved), Map("result" -> equals))(solved))
    val compiled = compileAndExecute(plan, Map("BAR" -> Double.box(9007199254740992D)))

    val result = getResult(compiled, "result")
    result.toSet should equal(Set(Map("result" -> false)))
  }

  test("or between two literal booleans") {
    val or = Or(True()(pos), False()(pos))(pos)
    val plan = ProduceResult(List("result"), Projection(SingleRow()(solved), Map("result" -> or))(solved))
    val compiled = compileAndExecute(plan)

    //then
    val result = getResult(compiled, "result")
    result.toSet should equal(Set(Map("result" -> true)))
  }

  test("or one from parameter") {
    val or = Or(False()(pos), Parameter("FOO", CTAny)(pos))(pos)
    val plan = ProduceResult(List("result"), Projection(SingleRow()(solved), Map("result" -> or))(solved))
    val compiled = compileAndExecute(plan, Map("FOO" -> Boolean.box(false)))

    //then
    val result = getResult(compiled, "result")
    result.toSet should equal(Set(Map("result" -> false)))
  }

  test("or two from parameter, one null") {
    val or = Or(Parameter("FOO", CTAny)(pos), Parameter("BAR", CTAny)(pos))(pos)
    val plan = ProduceResult(List("result"), Projection(SingleRow()(solved), Map("result" -> or))(solved))
    val compiled = compileAndExecute(plan, Map("FOO" -> Boolean.box(true), "BAR" -> null))

    //then
    val result = getResult(compiled, "result")
    result.toSet should equal(Set(Map("result" -> true)))
  }

  test("or two from parameter, both null") {
    val or = Or(Parameter("FOO", CTAny)(pos), Parameter("BAR", CTAny)(pos))(pos)
    val plan = ProduceResult(List("result"), Projection(SingleRow()(solved), Map("result" -> or))(solved))
    val compiled = compileAndExecute(plan, Map("FOO" -> null, "BAR" -> null))

    //then
    val result = getResult(compiled, "result")
    result.toSet should equal(Set(Map("result" -> null)))
  }

  test("not on a literal") {
    val not = Not(False()(pos))(pos)
    val plan = ProduceResult(List("result"), Projection(SingleRow()(solved), Map("result" -> not))(solved))
    val compiled = compileAndExecute(plan)

    //then
    val result = getResult(compiled, "result")
    result.toSet should equal(Set(Map("result" -> true)))
  }

  test("not on a parameter") {
    val not = Not(Parameter("FOO", CTAny)(pos))(pos)
    val plan = ProduceResult(List("result"), Projection(SingleRow()(solved), Map("result" -> not))(solved))
    val compiled = compileAndExecute(plan, Map("FOO" -> Boolean.box(false)))

    //then
    val result = getResult(compiled, "result")
    result.toSet should equal(Set(Map("result" -> true)))
  }

  test("not on a null parameter") {
    val not = Not(Parameter("FOO", CTAny)(pos))(pos)
    val plan = ProduceResult(List("result"), Projection(SingleRow()(solved), Map("result" -> not))(solved))
    val compiled = compileAndExecute(plan, Map("FOO" -> null))

    //then
    val result = getResult(compiled, "result")
    result.toSet should equal(Set(Map("result" -> null)))
  }

  test("close transaction after successfully exhausting result") {
    // given
    val plan = ProduceResult(List("a"), Projection(SingleRow()(solved), Map("a" -> SignedDecimalIntegerLiteral("1")(null)))(solved))

    // when
    val closer = mock[TaskCloser]
    val compiled = compileAndExecute(plan, taskCloser = closer)

    // then
    verifyZeroInteractions(closer)
    val visitor = mock[InternalResultVisitor[RuntimeException]]
    when(visitor.visit(any[InternalResultRow])).thenReturn(true)
    compiled.accept(visitor)
    verify(closer).close(success = true)
  }

  test("close transaction after prematurely terminating result exhaustion") {
    // given
    val plan = ProduceResult(List("a"), Projection(SingleRow()(solved), Map("a" -> SignedDecimalIntegerLiteral("1")(null)))(solved))

    // when
    val closer = mock[TaskCloser]
    val compiled = compileAndExecute(plan, taskCloser = closer)

    // then
    verifyZeroInteractions(closer)
    val visitor = mock[InternalResultVisitor[RuntimeException]]
    when(visitor.visit(any[InternalResultRow])).thenReturn(false)
    compiled.accept(visitor)
    verify(closer).close(success = true)
  }

  test("close transaction after failure while handling results") {
    // given
    val plan = ProduceResult(List("a"), Projection(SingleRow()(solved), Map("a" -> SignedDecimalIntegerLiteral("1")(null)))(solved))

    // when
    val closer = mock[TaskCloser]
    val compiled = compileAndExecute( plan, taskCloser = closer )

    // then
    verifyZeroInteractions(closer)
    val visitor = mock[InternalResultVisitor[RuntimeException]]
    val exception = new scala.RuntimeException()
    when(visitor.visit(any[InternalResultRow])).thenThrow(exception)
    intercept[RuntimeException] {
      compiled.accept(visitor)
    }
    verify(closer).close(success = false)
  }

  test("should throw the same error as the user provides") {
    // given
    val plan = ProduceResult(List("a"), Projection(SingleRow()(solved), Map("a" -> SignedDecimalIntegerLiteral("1")(null)))(solved))

    // when
    val closer = mock[TaskCloser]
    val compiled = compileAndExecute( plan, taskCloser = closer )

    // then
    val visitor = mock[InternalResultVisitor[RuntimeException]]
    val exception = new scala.RuntimeException()
    when(visitor.visit(any[InternalResultRow])).thenThrow(exception)
      try {
        compiled.accept(visitor)
        fail("should have thrown error")
      }
      catch {
        case e: Throwable => e should equal(exception)
      }

  }

  test("throw error when parameter is missing") {
    //given
    val plan = ProduceResult(List("a"), Projection(SingleRow()(solved), Map("a" -> Parameter("FOO", CTAny)(pos)))(solved))

    //when
    val compiled = compileAndExecute(plan)

    //then
    intercept[ParameterNotFoundException](getResult(compiled, "a"))
  }

  test("handle line breaks and double quotes in names") {
    //given
    val name = """{"a":
              |1
              |}
            """.stripMargin
    val plan = ProduceResult(List(name), Projection(SingleRow()(solved),
      Map(name -> SignedDecimalIntegerLiteral("1")(pos)))(solved))

    //when
    val compiled = compileAndExecute(plan, Map("FOO" -> Long.box(3L), "BAR" -> Long.box(1L)))

    //then
    val result = getResult(compiled, name)
    result.toSet should equal(Set(Map(name -> 1)))
  }

  test("count no grouping key") {
    when(semanticTable.resolvedPropertyKeyNames).thenReturn(mutable.Map.empty[String, PropertyKeyId])
    val scan = AllNodesScan(IdName("a"), Set.empty)(solved)
    val ns: Namespace = Namespace(List())(pos)
    val count: FunctionName = FunctionName("count")(pos)
    val property = Property(ast.Variable("a")(pos), PropertyKeyName("prop")(pos))(pos)
    val invocation: FunctionInvocation = FunctionInvocation(ns, count, distinct = false, Vector(property))(pos)
    val aggregation = Aggregation(scan, Map.empty, Map("count(a.prop)" -> invocation))(solved)
    val plan = ProduceResult(List("count(a.prop)"), aggregation)

    //when
    val compiled = compileAndExecute(plan)

    val result = getResult(compiled, "count(a.prop)")

    //then
    result.toList should equal(List(Map("count(a.prop)" -> 3L)))
  }

  test("count distinct no grouping key") {
    when(semanticTable.resolvedPropertyKeyNames).thenReturn(mutable.Map.empty[String, PropertyKeyId])
    val scan = AllNodesScan(IdName("a"), Set.empty)(solved)
    val ns: Namespace = Namespace(List())(pos)
    val count: FunctionName = FunctionName("count")(pos)
    val property = Property(ast.Variable("a")(pos), PropertyKeyName("prop")(pos))(pos)
    val invocation: FunctionInvocation = FunctionInvocation(ns, count, distinct = true, Vector(property))(pos)
    val aggregation = Aggregation(scan, Map.empty, Map("count(a.prop)" -> invocation))(solved)
    val plan = ProduceResult(List("count(a.prop)"), aggregation)

    //when
    val compiled = compileAndExecute(plan)

    val result = getResult(compiled, "count(a.prop)")

    //then
    result.toList should equal(List(Map("count(a.prop)" -> 1)))
  }

  test("count distinct no grouping key aggregate on node") {
    when(semanticTable.resolvedPropertyKeyNames).thenReturn(mutable.Map.empty[String, PropertyKeyId])
    val scan = AllNodesScan(IdName("a"), Set.empty)(solved)
    val ns: Namespace = Namespace(List())(pos)
    val count: FunctionName = FunctionName("count")(pos)
    val node = ast.Variable("a")(pos)
    val invocation: FunctionInvocation = FunctionInvocation(ns, count, distinct = true, Vector(node))(pos)
    val aggregation = Aggregation(scan, Map.empty, Map("count(a)" -> invocation))(solved)
    val plan = ProduceResult(List("count(a)"), aggregation)

    //when
    val compiled = compileAndExecute(plan)

    val result = getResult(compiled, "count(a)")

    //then
    result.toList should equal(List(Map("count(a)" -> 9)))
  }

  test("count node grouping key") {
    when(semanticTable.resolvedPropertyKeyNames).thenReturn(mutable.Map.empty[String, PropertyKeyId])
    val scan = AllNodesScan(IdName("a"), Set.empty)(solved)
    val ns: Namespace = Namespace(List())(pos)
    val count: FunctionName = FunctionName("count")(pos)
    val property = Property(ast.Variable("a")(pos), PropertyKeyName("prop")(pos))(pos)
    val invocation: FunctionInvocation = FunctionInvocation(ns, count, distinct = false, Vector(property))(pos)
    val aggregation = Aggregation(scan, Map("a" -> ast.Variable("a")(pos)), Map("count(a.prop)" -> invocation))(solved)
    val plan = ProduceResult(List("count(a.prop)"), aggregation)

    //when
    val compiled = compileAndExecute(plan)

    val result = getResult(compiled, "count(a.prop)")

    //then
    result.toList should equal(List(
      Map("count(a.prop)" -> 1),
      Map("count(a.prop)" -> 1),
      Map("count(a.prop)" -> 1),
      Map("count(a.prop)" -> 0),
      Map("count(a.prop)" -> 0),
      Map("count(a.prop)" -> 0),
      Map("count(a.prop)" -> 0),
      Map("count(a.prop)" -> 0),
      Map("count(a.prop)" -> 0))
    )
  }

  test("count distinct node grouping key") {
    when(semanticTable.resolvedPropertyKeyNames).thenReturn(mutable.Map.empty[String, PropertyKeyId])
    val scan = AllNodesScan(IdName("a"), Set.empty)(solved)
    val ns: Namespace = Namespace(List())(pos)
    val count: FunctionName = FunctionName("count")(pos)
    val property = Property(ast.Variable("a")(pos), PropertyKeyName("prop")(pos))(pos)
    val invocation: FunctionInvocation = FunctionInvocation(ns, count, distinct = true, Vector(property))(pos)
    val aggregation = Aggregation(scan, Map("a" -> ast.Variable("a")(pos)), Map("count(a.prop)" -> invocation))(solved)
    val plan = ProduceResult(List("count(a.prop)"), aggregation)

    //when
    val compiled = compileAndExecute(plan)

    val result = getResult(compiled, "count(a.prop)")

    //then
    result.toList should equal(List(
      Map("count(a.prop)" -> 1),
      Map("count(a.prop)" -> 1),
      Map("count(a.prop)" -> 1),
      Map("count(a.prop)" -> 0),
      Map("count(a.prop)" -> 0),
      Map("count(a.prop)" -> 0),
      Map("count(a.prop)" -> 0),
      Map("count(a.prop)" -> 0),
      Map("count(a.prop)" -> 0))
    )
  }

  test("count nodes distinct node grouping key") {
    when(semanticTable.resolvedPropertyKeyNames).thenReturn(mutable.Map.empty[String, PropertyKeyId])
    val scan = AllNodesScan(IdName("a"), Set.empty)(solved)
    val ns: Namespace = Namespace(List())(pos)
    val count: FunctionName = FunctionName("count")(pos)
    val node = ast.Variable("a")(pos)
    val invocation: FunctionInvocation = FunctionInvocation(ns, count, distinct = true, Vector(node))(pos)
    val aggregation = Aggregation(scan, Map("a" -> ast.Variable("a")(pos)), Map("count(a)" -> invocation))(solved)
    val plan = ProduceResult(List("count(a)"), aggregation)

    //when
    val compiled = compileAndExecute(plan)

    val result = getResult(compiled, "count(a)")

    //then


    result.toList should equal(List(Map("count(a)" -> 1),
                                    Map("count(a)" -> 1),
                                    Map("count(a)" -> 1),
                                    Map("count(a)" -> 1),
                                    Map("count(a)" -> 1),
                                    Map("count(a)" -> 1),
                                    Map("count(a)" -> 1),
                                    Map("count(a)" -> 1),
                                    Map("count(a)" -> 1))
    )
  }

  test("count property grouping key") {
    when(semanticTable.resolvedPropertyKeyNames).thenReturn(mutable.Map.empty[String, PropertyKeyId])
    val scan = AllNodesScan(IdName("a"), Set.empty)(solved)
    val ns: Namespace = Namespace(List())(pos)
    val count: FunctionName = FunctionName("count")(pos)
    val property: Expression = Property(ast.Variable("a")(pos), PropertyKeyName("prop")(pos))(pos)
    val invocation: FunctionInvocation = FunctionInvocation(ns, count, distinct = false, Vector(property))(pos)
    val projection = Projection(scan, Map("a.prop" -> property))(solved)

    val aggregation = Aggregation(projection, Map("a.prop" -> property), Map("count(a.prop)" -> invocation))(solved)

    val plan = ProduceResult(List("a.prop", "count(a.prop)"), aggregation)

    //when
    val compiled = compileAndExecute(plan)

    val result = getResult(compiled, "a.prop", "count(a.prop)")

    //then
    result.toList should equal(List(Map("a.prop" -> null, "count(a.prop)" -> 0),
                                    Map("a.prop" -> "value", "count(a.prop)" -> 3)))
  }

  test("count distinct property grouping key") {
    when(semanticTable.resolvedPropertyKeyNames).thenReturn(mutable.Map.empty[String, PropertyKeyId])
    val scan = AllNodesScan(IdName("a"), Set.empty)(solved)
    val ns: Namespace = Namespace(List())(pos)
    val count: FunctionName = FunctionName("count")(pos)
    val property: Expression = Property(ast.Variable("a")(pos), PropertyKeyName("prop")(pos))(pos)
    val invocation: FunctionInvocation = FunctionInvocation(ns, count, distinct = true, Vector(property))(pos)
    val projection = Projection(scan, Map("a.prop" -> property))(solved)

    val aggregation = Aggregation(projection, Map("a.prop" -> property), Map("count(a.prop)" -> invocation))(solved)

    val plan = ProduceResult(List("a.prop", "count(a.prop)"), aggregation)

    //when
    val compiled = compileAndExecute(plan)

    val result = getResult(compiled, "a.prop", "count(a.prop)")

    //then
    result.toList should equal(List(Map("a.prop" -> null, "count(a.prop)" -> 0),
                                    Map("a.prop" -> "value", "count(a.prop)" -> 1)))
  }

  test("count nodes distinct property grouping key") {
    when(semanticTable.resolvedPropertyKeyNames).thenReturn(mutable.Map.empty[String, PropertyKeyId])
    val scan = AllNodesScan(IdName("a"), Set.empty)(solved)
    val ns: Namespace = Namespace(List())(pos)
    val count: FunctionName = FunctionName("count")(pos)
    val property: Expression = Property(ast.Variable("a")(pos), PropertyKeyName("prop")(pos))(pos)
    val node = ast.Variable("a")(pos)
    val invocation: FunctionInvocation = FunctionInvocation(ns, count, distinct = true, Vector(node))(pos)
    val projection = Projection(scan, Map("a.prop" -> property, "a" -> node))(solved)

    val aggregation = Aggregation(projection, Map("a.prop" -> property), Map("count(a)" -> invocation))(solved)

    val plan = ProduceResult(List("a.prop", "count(a)"), aggregation)

    //when
    val compiled = compileAndExecute(plan)

    val result = getResult(compiled, "a.prop", "count(a)")

    //then
    result.toList should equal(List(Map("a.prop" -> null, "count(a)" -> 6), Map("a.prop" -> "value", "count(a)" -> 3)))
  }

  test("unwind list of integers") { // UNWIND [1, 2, 3] as x RETURN x
    // given
    val listLiteral = ListLiteral(Seq(
      SignedDecimalIntegerLiteral("1")(pos),
      SignedDecimalIntegerLiteral("2")(pos),
      SignedDecimalIntegerLiteral("3")(pos)
    ))(pos)

    val unwind = UnwindCollection(SingleRow()(solved), IdName("x"), listLiteral)(solved)
    val plan = ProduceResult(List("x"), unwind)

    // when
    val compiled = compileAndExecute(plan)

    // then
    val result = getResult(compiled, "x")
    result.toList should equal(List(Map("x" -> 1L), Map("x" -> 2L), Map("x" -> 3L)))
  }

  test("unwind list of integers with projection") { // UNWIND [1, 2, 3] as x WITH x as y RETURN y
    // given
    val listLiteral = literalIntList(1, 2, 3)

    val unwind = UnwindCollection(SingleRow()(solved), IdName("x"), listLiteral)(solved)
    val projection = Projection(unwind, Map("y" -> varFor("x")))(solved)
    val plan = ProduceResult(List("y"), projection)

    // when
    val compiled = compileAndExecute(plan)

    // then
    val result = getResult(compiled, "y")
    result.toList should equal(List(Map("y" -> 1L), Map("y" -> 2L), Map("y" -> 3L)))
  }

  test("projection of int list") { // WITH [1, 2, 3] as x RETURN x
    // given
    val listLiteral = literalIntList(1, 2, 3)

    val projection = Projection(SingleRow()(solved), Map("x" -> listLiteral))(solved)
    val plan = ProduceResult(List("x"), projection)

    // when
    val compiled = compileAndExecute(plan)

    // then
    val result = getResult(compiled, "x")
    result.toList should equal(List(Map("x" -> List(1L, 2L, 3L))))
  }

  test("compare nullable relationships with equals") {
    //given
    val scanT1 = NodeByLabelScan(IdName("a"), lblName("T1"), Set.empty)(solved)
    val expand = Expand(scanT1,
      from = IdName("a"), SemanticDirection.OUTGOING, Seq.empty,
      to = IdName("b"), relName = IdName("r1"), ExpandAll)(solved)
    val optionalExpandInto = OptionalExpand(expand,
      from = IdName("b"), SemanticDirection.OUTGOING, Seq.empty,
      to = IdName("a"), IdName("r2"), ExpandInto, Seq(HasLabels(varFor("a"), Seq(LabelName("T2")(pos)))(pos)))(solved)

    val projection = Projection(optionalExpandInto, Map("sameRel" -> Equals(varFor("r1"), varFor("r2"))(pos)))(solved)
    val plan = ProduceResult(List("sameRel"), projection)

    //when
    val compiled = compileAndExecute(plan)

    //then
    val result = getResult(compiled, "sameRel")

    // Since r2 is null the result of equals should be null
    result should equal(List(
      Map("sameRel" -> null),
      Map("sameRel" -> null),
      Map("sameRel" -> null)
    ))
  }

  private def compile(plan: LogicalPlan) = {
    generator.generate(plan, newMockedPlanContext, semanticTable, CostBasedPlannerName.default)
  }

  private def compileAndExecute(plan: LogicalPlan, params: Map[String, AnyRef] = Map.empty, taskCloser: TaskCloser = new TaskCloser) = {
    compile(plan).executionResultBuilder(queryContext, NormalMode, tracer(NormalMode), params, taskCloser)
  }

  /*
   * Mocks the following graph:
   *
   * (a:T1) -[:R1] ->(d)<-[:R2]-(f:T2)
   *               ↗
   * (b:T1) -[:R1]
   *
   * (c:T1) -[:R1] ->(e)<-[:R2]-(g:T2)
   *
   * (h:T3)-(t2:T3)-(g:T3)
   *
   */
  private val labelTokens = Map("T1" -> 1, "T2" -> 2, "T3" -> 3)
  private val relTokens = Map("R1" -> 1, "R2" -> 2, "R3" -> 3)

  private val aNode = mockNode(0L, "a")
  private val bNode = mockNode(1L, "b")
  private val cNode = mockNode(2L, "c")
  private val dNode = mockNode(3L, "d")
  private val eNode = mockNode(4L, "e")
  private val fNode = mockNode(5L, "f")
  private val gNode = mockNode(6L, "g")
  private val hNode = mockNode(7L, "h")
  private val iNode = mockNode(8L, "i")

  private val semanticTable =  mock[SemanticTable]
  when(semanticTable.isNode(varFor("a"))).thenReturn(true)
  when(semanticTable.isNode(varFor("b"))).thenReturn(true)
  when(semanticTable.isNode(varFor("c"))).thenReturn(true)
  when(semanticTable.isNode(varFor("d"))).thenReturn(true)
  when(semanticTable.isNode(varFor("e"))).thenReturn(true)
  when(semanticTable.isNode(varFor("f"))).thenReturn(true)
  when(semanticTable.isNode(varFor("g"))).thenReturn(true)
  when(semanticTable.isNode(varFor("h"))).thenReturn(true)
  when(semanticTable.isNode(varFor("i"))).thenReturn(true)
  when(semanticTable.isRelationship(varFor("r1"))).thenReturn(true)
  when(semanticTable.isRelationship(varFor("r2"))).thenReturn(true)
  // x, y, z reserved for variables that are not node or relationship
  when(semanticTable.isNode(varFor("x"))).thenReturn(false)
  when(semanticTable.isRelationship(varFor("x"))).thenReturn(false)
  when(semanticTable.isNode(varFor("y"))).thenReturn(false)
  when(semanticTable.isRelationship(varFor("y"))).thenReturn(false)
  when(semanticTable.isNode(varFor("z"))).thenReturn(false)
  when(semanticTable.isRelationship(varFor("z"))).thenReturn(false)

  private val allNodes = Seq(aNode, bNode, cNode, dNode, eNode, fNode, gNode, hNode, iNode)
  private val nodesForLabel = Map("T1" -> Seq(aNode, bNode, cNode), "T2" -> Seq(fNode, gNode), "T3" -> Seq(hNode, iNode))

  private val relMap = Map(
    11L -> RelationshipData(aNode, dNode, 11L, 1),
    12L -> RelationshipData(bNode, dNode, 12L, 1),
    13L -> RelationshipData(cNode, eNode, 13L, 1),
    14L -> RelationshipData(fNode, dNode, 14L, 2),
    15L -> RelationshipData(gNode, eNode, 15L, 2),
    16L -> RelationshipData(hNode, iNode, 16L, 3),
    17L -> RelationshipData(iNode, hNode, 17L, 3))

  val nodeManager = mock[NodeManager]
  when(nodeManager.newNodeProxyById(anyLong())).thenAnswer(new Answer[Node]() {
    override def answer(invocationOnMock: InvocationOnMock): NodeProxy = {
      val id = invocationOnMock.getArguments.apply(0).asInstanceOf[Long].toInt
      allNodes(id)
    }
  })
  when(nodeManager.newRelationshipProxyById(anyLong())).thenAnswer(new Answer[Relationship]() {
    override def answer(invocationOnMock: InvocationOnMock): RelationshipProxy = {
      val id = invocationOnMock.getArguments.apply(0).asInstanceOf[Long].toInt
      relMap(id).relationship
    }
  })

  private val queryContext = mock[QueryContext]
  private val transactionalContext = mock[TransactionalContextWrapper]
  private val ro = mock[ReadOperations]
  when(queryContext.transactionalContext).thenReturn(transactionalContext)
  when(transactionalContext.readOperations).thenReturn(ro)
  when(queryContext.entityAccessor).thenReturn(nodeManager.asInstanceOf[queryContext.EntityAccessor])
  when(ro.nodeGetProperty(anyLong(), anyInt())).thenAnswer(new Answer[Object] {
    override def answer(invocationOnMock: InvocationOnMock): Object = {
      val id = invocationOnMock.getArguments()(0).asInstanceOf[Long]
      if (id < 3) "value"
      else null
    }
  })
  when(ro.nodesGetAll()).thenAnswer(new Answer[PrimitiveLongIterator] {
    override def answer(invocationOnMock: InvocationOnMock): PrimitiveLongIterator = primitiveIterator(allNodes.map(_.getId))
  })
  when(ro.labelGetForName(anyString())).thenAnswer(new Answer[Int] {
    override def answer(invocationOnMock: InvocationOnMock): Int = {
      val label = invocationOnMock.getArguments.apply(0).asInstanceOf[String]
      labelTokens(label)
    }
  })
  when(ro.relationshipTypeGetForName(anyString())).thenAnswer(new Answer[Int] {
    override def answer(invocationOnMock: InvocationOnMock): Int = {
      val label = invocationOnMock.getArguments.apply(0).asInstanceOf[String]
      relTokens(label)
    }
  })
  when(ro.nodesGetForLabel(anyInt())).thenAnswer(new Answer[PrimitiveLongIterator] {
    override def answer(invocationOnMock: InvocationOnMock): PrimitiveLongIterator = {
      val labelToken = invocationOnMock.getArguments.apply(0).asInstanceOf[Int]
      val (label, _) = labelTokens.find {
        case (l, t) => t == labelToken
      }.get
      val nodeIds = nodesForLabel(label).map(_.getId)
      primitiveIterator(nodeIds)
    }
  })
  when(ro.nodeGetRelationships(anyLong(), any[Direction])).thenAnswer(new Answer[PrimitiveLongIterator] {
    override def answer(invocationOnMock: InvocationOnMock): PrimitiveLongIterator = {
      val node = invocationOnMock.getArguments.apply(0).asInstanceOf[Long].toInt
      val dir = invocationOnMock.getArguments.apply(1).asInstanceOf[Direction]
      getRelsForNode(allNodes(node), dir, Set.empty)
    }
  })
  when(ro.nodeGetRelationships(anyLong(), any[Direction], anyVararg[Int]())).thenAnswer(new Answer[PrimitiveLongIterator] {
    override def answer(invocationOnMock: InvocationOnMock): PrimitiveLongIterator = {
      val arguments = invocationOnMock.getArguments
      val node = arguments(0).asInstanceOf[Long].toInt
      val dir = arguments(1).asInstanceOf[Direction]
      val types = (2 until arguments.length).map(arguments(_).asInstanceOf[Int]).toSet
      getRelsForNode(allNodes(node), dir, types)
    }
  })
  when(ro.relationshipVisit(anyLong(), any())).thenAnswer(new Answer[Unit] {
    override def answer(invocationOnMock: InvocationOnMock): Unit = {
      val relId = invocationOnMock.getArguments.apply(0).asInstanceOf[Long]
      val visitor = invocationOnMock.getArguments.apply(1).asInstanceOf[RelationshipVisitor[_]]
      val rel = relMap(relId)
      visitor.visit(relId, -1, rel.from.getId, rel.to.getId)
    }
  })
  when(ro.nodeGetDegree(anyLong(), any())).thenReturn(1)
  when(ro.nodeGetDegree(anyLong(), any(), anyInt())).thenReturn(1)
  when(ro.nodeHasLabel(anyLong(), anyInt())).thenAnswer(new Answer[Boolean] {
    override def answer(invocationOnMock: InvocationOnMock) = {
      val nodeId = invocationOnMock.getArguments.apply(0).asInstanceOf[Long]
      val labelId = invocationOnMock.getArguments.apply(1).asInstanceOf[Int]
      val label = labelTokens.map {
        case (labelName, t) if t == labelId => Some(labelName)
        case _ => None
      }.head
      label.exists(l => nodesForLabel.contains(l) && nodesForLabel(l).exists(_.getId == nodeId)
      )
    }
  })

  private def mockNode(id: Long, name: String) = {
    val node = mock[NodeProxy]
    when(node.getId).thenReturn(id)
    when(node.toString).thenReturn(name)
    node
  }

  private def mockRelationship(relationshipData: RelationshipData) = {
    val rel = mock[RelationshipProxy]
    val toStringValue = relationshipData.toString
    when(rel.getId).thenReturn(relationshipData.id)
    when(rel.getStartNode).thenReturn(relationshipData.from)
    when(rel.getEndNode).thenReturn(relationshipData.to)
    when(rel.toString).thenReturn(toStringValue)
    rel
  }

  private def getRelsForNode(node: Node, dir: Direction, types: Set[Int]) = {
    def hasType(relationship: RelationshipData) = types.isEmpty || types(relationship.relType)
    if (dir == Direction.OUTGOING) {
      val relIds = relMap.values.filter(n => n.from == node && hasType(n)).map(_.id).toSeq
      relationshipIterator(relIds)
    } else if (dir == Direction.INCOMING) {
      val relIds = relMap.values.filter(n => n.to == node && hasType(n)).map(_.id).toSeq
      relationshipIterator(relIds)
    } else {
      val relIds = relMap.values.filter(n => (n.from == node || n.to == node) && hasType(n)).map(_.id).toSeq
      relationshipIterator(relIds)
    }
  }

  case class RelationshipData(from: Node, to: Node, id: Long, relType: Int) {
    val relationship = mockRelationship(this)

    override def toString: String = s"($from)-[$relType]->($to)"
  }

  private def primitiveIterator(longs: Seq[Long]) = new PrimitiveLongIterator {
    val inner = longs.toIterator

    override def next(): Long = inner.next()

    override def hasNext: Boolean = inner.hasNext
  }

  private def relationshipIterator(longs: Seq[Long]) = new RelationshipIterator {

    override def relationshipVisit[EXCEPTION <: Exception](relationshipId: Long, visitor: RelationshipVisitor[EXCEPTION]): Boolean = {
      val rel = relMap(relationshipId)
      visitor.visit(relationshipId, -1, rel.from.getId, rel.to.getId)
      false
    }

    val inner = longs.toIterator

    override def next(): Long = inner.next()

    override def hasNext: Boolean = inner.hasNext
  }

  private def getNodesFromResult(plan: InternalExecutionResult, columns: String*) = {
    val res = Seq.newBuilder[Map[String, Node]]

    plan.accept(new InternalResultVisitor[RuntimeException]() {
      override def visit(element: InternalResultRow): Boolean = {
        res += columns.map(col => col -> element.getNode(col)).toMap
        true
      }
    })
    res.result()
  }

  private def getResult(plan: InternalExecutionResult, columns: String*) = {
    val res = Seq.newBuilder[Map[String, Any]]

    plan.accept(new InternalResultVisitor[RuntimeException]() {
      override def visit(element: InternalResultRow): Boolean = {
        res += columns.map(col => col -> element.get(col)).toMap
        true
      }
    })
    res.result().asComparableResult
  }


  implicit class RichMapSeq(res: Seq[Map[String, Any]]) {
    import JavaConverters._

    def asComparableResult: Seq[Map[String, Any]] = res.map((map: Map[String, Any]) =>
      map.map {
        case (k, a: Array[_]) => k -> a.toList
        case (k, a: java.util.List[_]) => k -> a.asScala
        case (k, m: java.util.Map[_,_]) => k -> m.asScala
        case m => m
      }
    )
  }
}
