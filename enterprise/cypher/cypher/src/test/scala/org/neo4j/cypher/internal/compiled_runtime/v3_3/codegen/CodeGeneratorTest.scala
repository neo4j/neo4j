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
package org.neo4j.cypher.internal.compiled_runtime.v3_3.codegen

import java.util
import java.util.function.BiConsumer

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.collection.primitive.PrimitiveLongIterator
import org.neo4j.cypher.internal.InternalExecutionResult
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.compiled.ExecutionPlanBuilder.tracer
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.compiled.codegen.{ByteCodeMode, CodeGenConfiguration, CodeGenerator, SourceCodeMode}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{NormalMode, TaskCloser}
import org.neo4j.cypher.internal.compiler.v3_3.CostBasedPlannerName
import org.neo4j.cypher.internal.compiler.v3_3.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.neo4j.cypher.internal.frontend.v3_3.symbols._
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_3.{ParameterNotFoundException, SemanticDirection, SemanticTable, _}
import org.neo4j.cypher.internal.ir.v3_3.IdName
import org.neo4j.cypher.internal.spi.v3_3.codegen.GeneratedQueryStructure
import org.neo4j.cypher.internal.spi.v3_3.{QueryContext, TransactionalContextWrapper}
import org.neo4j.cypher.internal.v3_3.logical.plans
import org.neo4j.cypher.internal.v3_3.logical.plans.{Ascending, Descending, _}
import org.neo4j.graphdb.Result.{ResultRow, ResultVisitor}
import org.neo4j.graphdb._
import org.neo4j.helpers.ValueUtils
import org.neo4j.kernel.api.ReadOperations
import org.neo4j.kernel.impl.api.RelationshipVisitor
import org.neo4j.kernel.impl.api.store.RelationshipIterator
import org.neo4j.kernel.impl.core.{NodeManager, NodeProxy, RelationshipProxy}
import org.neo4j.time.Clocks
import org.neo4j.values.AnyValue
import org.neo4j.values.storable._
import org.neo4j.values.virtual.VirtualValues.EMPTY_MAP
import org.neo4j.values.virtual.{EdgeValue, ListValue, MapValue, NodeValue}

import scala.collection.JavaConverters._
import scala.collection.{JavaConverters, mutable}

class ByteCodeGeneratorTest extends CodeGeneratorTest {
  override val generator =
    new CodeGenerator(GeneratedQueryStructure, Clocks.systemClock(), CodeGenConfiguration(mode = ByteCodeMode))
}

class SourceCodeGeneratorTest extends CodeGeneratorTest {
  override val generator =
    new CodeGenerator(GeneratedQueryStructure, Clocks.systemClock(), CodeGenConfiguration(mode = SourceCodeMode))
}

abstract class CodeGeneratorTest extends CypherFunSuite with LogicalPlanningTestSupport {

  protected val generator: CodeGenerator

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

  test("all nodes scan + expand + projection") { // MATCH (a)-[r]->(b) WITH a, b RETURN a, b, 1
    //given
    val plan = ProduceResult(List("a", "b", "1"),
      plans.Projection(
        Expand(
          AllNodesScan(IdName("a"), Set.empty)(solved), IdName("a"),
          SemanticDirection.OUTGOING, Seq.empty, IdName("b"), IdName("r"), ExpandAll)(solved),
        Map("a" -> varFor("a"),
          "b" -> varFor("b"),
          "1" -> Parameter("  AUTOINT0", CTInteger)(pos)))(solved))

    //when
    val compiled = compileAndExecute(plan, param("  AUTOINT0" -> Long.box(1L)))

    //then
    val result = getResult(compiled, "a", "b", "1")

    result should equal(List(
      Map("a" -> aNode, "b" -> dNode, "1" -> 1L),
      Map("a" -> bNode, "b" -> dNode, "1" -> 1L),
      Map("a" -> cNode, "b" -> eNode, "1" -> 1L),
      Map("a" -> fNode, "b" -> dNode, "1" -> 1L),
      Map("a" -> gNode, "b" -> eNode, "1" -> 1L),
      Map("a" -> hNode, "b" -> iNode, "1" -> 1L),
      Map("a" -> iNode, "b" -> hNode, "1" -> 1L)))
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
    val projection = plans.Projection(join2, Map("a" -> varFor("a")))(solved)
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
    val plan = ProduceResult(List("a"), plans.Projection(SingleRow()(solved), Map("a" -> SignedDecimalIntegerLiteral("1")(pos)))(solved))
    val compiled = compileAndExecute(plan)

    //then
    val result = getResult(compiled, "a")
    result.toSet should equal(Set(Map("a" -> 1)))
  }

  test("project parameter") {

    val plan = ProduceResult(List("a"), plans.Projection(SingleRow()(solved), Map("a" -> Parameter("FOO", CTAny)(pos)))(solved))
    val compiled = compileAndExecute(plan, param("FOO" -> "BAR"))

    //then
    val result = getResult(compiled, "a")
    result.toSet should equal(Set(Map("a" -> "BAR")))
  }

  test("project null parameters") {

    val plan = ProduceResult(List("a"), plans.Projection(SingleRow()(solved), Map("a" -> Parameter("FOO", CTAny)(pos)))(solved))
    val compiled = compileAndExecute(plan, param("FOO" -> null))

    //then
    val result = getResult(compiled, "a")
    result.toSet should equal(Set(Map("a" -> null)))
  }

  test("project primitive parameters") {

    val plan = ProduceResult(List("a", "r1", "x", "y", "z"),
      plans.Projection(SingleRow()(solved), Map("a" -> Parameter("FOO_NODE", CTNode)(pos),
                                                "r1" -> Parameter("FOO_REL",  CTRelationship)(pos),
                                                "x" -> Parameter("FOO1", CTInteger)(pos),
                                                "y" -> Parameter("FOO2", CTFloat)(pos),
                                                "z" -> Parameter("FOO3", CTBoolean)(pos)))(solved))

    val compiled = compileAndExecute(plan, param("FOO_NODE" -> aNode,
                                               "FOO_REL" -> relMap(11L).relationship,
                                               "FOO1" -> 42L.asInstanceOf[AnyRef],
                                               "FOO2" -> 3.14d.asInstanceOf[AnyRef],
                                               "FOO3" -> true.asInstanceOf[AnyRef]))

    //then
    val result = getResult(compiled, "a", "r1", "x", "y", "z")
    result.toSet should equal(Set(Map("a" -> aNode, "r1" -> relMap(11L).relationship,
                                      "x" -> 42L, "y" -> 3.14d, "z" -> true)))
  }

  test("project null primitive node and relationship parameters") {

    val plan = ProduceResult(List("a", "r1"),
      plans.Projection(SingleRow()(solved), Map("a" -> Parameter("FOO_NODE", CTNode)(pos),
                                               "r1" -> Parameter("FOO_REL",  CTRelationship)(pos)))(solved))

    val compiled = compileAndExecute(plan, param("FOO_NODE" -> null,
                                               "FOO_REL" -> null))

    //then
    val result = getResult(compiled, "a", "r1")
    result.toSet should equal(Set(Map("a" -> null, "r1" -> null)))
  }

  test("project nodes") {
    val scan = AllNodesScan(IdName("a"), Set.empty)(solved)
    val plan = ProduceResult(List("a"), plans.Projection(scan, Map("a" -> varFor("a")))(solved))
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
    val projection = plans.Projection(expand, Map("r" -> varFor("r")))(solved)
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

    val plan = ProduceResult(List("a"), plans.Projection(SingleRow()(solved), Map("a" -> add))(solved))
    val compiled = compileAndExecute(plan)

    //then
    val result = getResult(compiled, "a")
    result.toSet should equal(Set(Map("a" -> 4)))
  }

  test("project subtraction of two ints") {
    val lhs = SignedDecimalIntegerLiteral("7")(pos)
    val rhs = SignedDecimalIntegerLiteral("5")(pos)
    val subtract = Subtract(lhs, rhs)(pos)

    val plan = ProduceResult(List("a"), plans.Projection(SingleRow()(solved), Map("a" -> subtract))(solved))
    val compiled = compileAndExecute(plan)

    //then
    val result = getResult(compiled, "a")
    result.toSet should equal(Set(Map("a" -> 2)))
  }

  test("project addition of int and double") {
    val lhs = SignedDecimalIntegerLiteral("1")(pos)
    val rhs = DecimalDoubleLiteral("3.0")(pos)
    val add = Add(lhs, rhs)(pos)

    val plan = ProduceResult(List("a"), plans.Projection(SingleRow()(solved), Map("a" -> add))(solved))
    val compiled = compileAndExecute(plan)

    //then
    val result = getResult(compiled, "a")
    result.toSet should equal(Set(Map("a" -> (1L + 3.0))))
  }

  test("project addition of int and String") {
    val lhs = SignedDecimalIntegerLiteral("1")(pos)
    val rhs = StringLiteral("two")(pos)
    val add = Add(lhs, rhs)(pos)

    val plan = ProduceResult(List("a"), plans.Projection(SingleRow()(solved), Map("a" -> add))(solved))
    val compiled = compileAndExecute(plan)

    //then
    val result = getResult(compiled, "a")
    result.toSet should equal(Set(Map("a" -> "1two")))
  }

  test("project addition of int and value from params") {
    val lhs = SignedDecimalIntegerLiteral("1")(pos)
    val rhs = Parameter("FOO", CTAny)(pos)
    val add = Add(lhs, rhs)(pos)
    val plan = ProduceResult(List("a"), plans.Projection(SingleRow()(solved), Map("a" -> add))(solved))
    val compiled = compileAndExecute(plan, param("FOO" -> Long.box(3L)))

    //then
    val result = getResult(compiled, "a")
    result.toSet should equal(Set(Map("a" -> 4)))
  }

  test("project addition of two values coming from params") {
    val lhs = Parameter("FOO", CTAny)(pos)
    val rhs = Parameter("BAR", CTAny)(pos)
    val add = Add(lhs, rhs)(pos)
    val plan = ProduceResult(List("a"), plans.Projection(SingleRow()(solved), Map("a" -> add))(solved))
    val compiled = compileAndExecute(plan, param("FOO" -> Long.box(3L), "BAR" -> Long.box(1L)))

    //then
    val result = getResult(compiled, "a")
    result.toSet should equal(Set(Map("a" -> 4)))
  }

  test("project collection") {
    val collection = ListLiteral(Seq(Parameter("FOO", CTAny)(pos), Parameter("BAR", CTAny)(pos)))(pos)
    val plan = ProduceResult(List("a"), plans.Projection(SingleRow()(solved), Map("a" -> collection))(solved))
    val compiled = compileAndExecute(plan, param("FOO" -> Long.box(3L), "BAR" -> Long.box(1L)))

    //then
    val result = getResult(compiled, "a")
    result.toSet should equal(Set(Map("a" -> List(3, 1))))
  }

  test("project map") {
    val map = MapExpression(Seq((PropertyKeyName("FOO")(pos), Parameter("BAR", CTAny)(pos))))(pos)
    val plan = ProduceResult(List("a"), plans.Projection(SingleRow()(solved), Map("a" -> map))(solved))
    val compiled = compileAndExecute(plan, param("FOO" -> Long.box(3L), "BAR" -> Long.box(1L)))

    //then
    val result = getResult(compiled, "a")
    result.toSet should equal(Set(Map("a" -> Map("FOO" -> 1))))
  }

  test("string equality") {
    val equals = Equals(StringLiteral("a string")(pos), StringLiteral("a string")(pos))(pos)
    val plan = ProduceResult(List("result"), plans.Projection(SingleRow()(solved), Map("result" -> equals))(solved))
    val compiled = compileAndExecute(plan)

    //then
    val result = getResult(compiled, "result")
    result.toSet should equal(Set(Map("result" -> true)))
  }

  test("number equality, double and long") {
    val equals = Equals(SignedDecimalIntegerLiteral("9007199254740993")(pos), DecimalDoubleLiteral("9007199254740992")(pos))(pos)
    val plan = ProduceResult(List("result"), plans.Projection(SingleRow()(solved), Map("result" -> equals))(solved))
    val compiled = compileAndExecute(plan)

    //then
    val result = getResult(compiled, "result")
    result.toSet should equal(Set(Map("result" -> false)))
  }

  test("number equality, long and long") {
    val equals = Equals(SignedDecimalIntegerLiteral("9007199254740993")(pos), SignedDecimalIntegerLiteral("9007199254740992")(pos))(pos)
    val plan = ProduceResult(List("result"), plans.Projection(SingleRow()(solved), Map("result" -> equals))(solved))
    val compiled = compileAndExecute(plan)

    //then
    val result = getResult(compiled, "result")
    result.toSet should equal(Set(Map("result" -> false)))
  }

  test("number equality, one from parameter") {
    val equals = Equals(SignedDecimalIntegerLiteral("9007199254740993")(pos), Parameter("BAR", CTAny)(pos))(pos)
    val plan = ProduceResult(List("result"), plans.Projection(SingleRow()(solved), Map("result" -> equals))(solved))
    val compiled = compileAndExecute(plan, param("BAR" -> Double.box(9007199254740992D)))

    val result = getResult(compiled, "result")
    result.toSet should equal(Set(Map("result" -> false)))
  }

  test("or between two literal booleans") {
    val or = Or(True()(pos), False()(pos))(pos)
    val plan = ProduceResult(List("result"), plans.Projection(SingleRow()(solved), Map("result" -> or))(solved))
    val compiled = compileAndExecute(plan)

    //then
    val result = getResult(compiled, "result")
    result.toSet should equal(Set(Map("result" -> true)))
  }

  test("or one from parameter") {
    val or = Or(False()(pos), Parameter("FOO", CTAny)(pos))(pos)
    val plan = ProduceResult(List("result"), plans.Projection(SingleRow()(solved), Map("result" -> or))(solved))
    val compiled = compileAndExecute(plan, param("FOO" -> Boolean.box(false)))

    //then
    val result = getResult(compiled, "result")
    result.toSet should equal(Set(Map("result" -> false)))
  }

  test("or two from parameter, one null") {
    val or = Or(Parameter("FOO", CTAny)(pos), Parameter("BAR", CTAny)(pos))(pos)
    val plan = ProduceResult(List("result"), plans.Projection(SingleRow()(solved), Map("result" -> or))(solved))
    val compiled = compileAndExecute(plan, param("FOO" -> Boolean.box(true), "BAR" -> null))

    //then
    val result = getResult(compiled, "result")
    result.toSet should equal(Set(Map("result" -> true)))
  }

  test("or two from parameter, both null") {
    val or = Or(Parameter("FOO", CTAny)(pos), Parameter("BAR", CTAny)(pos))(pos)
    val plan = ProduceResult(List("result"), plans.Projection(SingleRow()(solved), Map("result" -> or))(solved))
    val compiled = compileAndExecute(plan, param("FOO" -> null, "BAR" -> null))

    //then
    val result = getResult(compiled, "result")
    result.toSet should equal(Set(Map("result" -> null)))
  }

  test("not on a literal") {
    val not = Not(False()(pos))(pos)
    val plan = ProduceResult(List("result"), plans.Projection(SingleRow()(solved), Map("result" -> not))(solved))
    val compiled = compileAndExecute(plan)

    //then
    val result = getResult(compiled, "result")
    result.toSet should equal(Set(Map("result" -> true)))
  }

  test("not on a parameter") {
    val not = Not(Parameter("FOO", CTAny)(pos))(pos)
    val plan = ProduceResult(List("result"), plans.Projection(SingleRow()(solved), Map("result" -> not))(solved))
    val compiled = compileAndExecute(plan, param("FOO" -> Boolean.box(false)))

    //then
    val result = getResult(compiled, "result")
    result.toSet should equal(Set(Map("result" -> true)))
  }

  test("not on a null parameter") {
    val not = Not(Parameter("FOO", CTAny)(pos))(pos)
    val plan = ProduceResult(List("result"), plans.Projection(SingleRow()(solved), Map("result" -> not))(solved))
    val compiled = compileAndExecute(plan, param("FOO" -> null))

    //then
    val result = getResult(compiled, "result")
    result.toSet should equal(Set(Map("result" -> null)))
  }

  test("close transaction after successfully exhausting result") {
    // given
    val plan = ProduceResult(List("a"), plans.Projection(SingleRow()(solved), Map("a" -> SignedDecimalIntegerLiteral("1")(null)))(solved))

    // when
    val closer = mock[TaskCloser]
    val compiled = compileAndExecute(plan, taskCloser = closer)

    // then
    verifyZeroInteractions(closer)
    val visitor = mock[ResultVisitor[RuntimeException]]
    when(visitor.visit(any[ResultRow])).thenReturn(true)
    compiled.accept(visitor)
    verify(closer).close(success = true)
  }

  test("close transaction after prematurely terminating result exhaustion") {
    // given
    val plan = ProduceResult(List("a"), plans.Projection(SingleRow()(solved), Map("a" -> SignedDecimalIntegerLiteral("1")(null)))(solved))

    // when
    val closer = mock[TaskCloser]
    val compiled = compileAndExecute(plan, taskCloser = closer)

    // then
    verifyZeroInteractions(closer)
    val visitor = mock[ResultVisitor[RuntimeException]]
    when(visitor.visit(any[ResultRow])).thenReturn(false)
    compiled.accept(visitor)
    verify(closer).close(success = true)
  }

  test("close transaction after failure while handling results") {
    // given
    val plan = ProduceResult(List("a"), plans.Projection(SingleRow()(solved), Map("a" -> SignedDecimalIntegerLiteral("1")(null)))(solved))

    // when
    val closer = mock[TaskCloser]
    val compiled = compileAndExecute( plan, taskCloser = closer )

    // then
    verifyZeroInteractions(closer)
    val visitor = mock[ResultVisitor[RuntimeException]]
    val exception = new scala.RuntimeException()
    when(visitor.visit(any[ResultRow])).thenThrow(exception)
    intercept[RuntimeException] {
      compiled.accept(visitor)
    }
    verify(closer).close(success = false)
  }

  test("should throw the same error as the user provides") {
    // given
    val plan = ProduceResult(List("a"), plans.Projection(SingleRow()(solved), Map("a" -> SignedDecimalIntegerLiteral("1")(null)))(solved))

    // when
    val closer = mock[TaskCloser]
    val compiled = compileAndExecute( plan, taskCloser = closer )

    // then
    val visitor = mock[ResultVisitor[RuntimeException]]
    val exception = new scala.RuntimeException()
    when(visitor.visit(any[ResultRow])).thenThrow(exception)
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
    val plan = ProduceResult(List("a"), plans.Projection(SingleRow()(solved), Map("a" -> Parameter("FOO", CTAny)(pos)))(solved))

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
    val plan = ProduceResult(List(name), plans.Projection(SingleRow()(solved),
      Map(name -> SignedDecimalIntegerLiteral("1")(pos)))(solved))

    //when
    val compiled = compileAndExecute(plan, param("FOO" -> Long.box(3L), "BAR" -> Long.box(1L)))

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

    val aggregation = Aggregation(scan, Map("a.prop" -> property), Map("count(a.prop)" -> invocation))(solved)

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
    val aggregation = Aggregation(scan, Map("a.prop" -> property), Map("count(a.prop)" -> invocation))(solved)

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

    val aggregation = Aggregation(scan, Map("a.prop" -> property), Map("count(a)" -> invocation))(solved)

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

    val unwind = plans.UnwindCollection(SingleRow()(solved), IdName("x"), listLiteral)(solved)
    val plan = ProduceResult(List("x"), unwind)

    // when
    val compiled = compileAndExecute(plan)

    // then
    val result = getResult(compiled, "x")
    result.toList should equal(List(Map("x" -> 1L), Map("x" -> 2L), Map("x" -> 3L)))
  }

  test("unwind list of floats") { // UNWIND [1.0, 2.0, 3.0] as x RETURN x
    // given
    val listLiteral = ListLiteral(Seq(
      DecimalDoubleLiteral("1.0")(pos),
      DecimalDoubleLiteral("2.0")(pos),
      DecimalDoubleLiteral("3.0")(pos)
    ))(pos)

    val unwind = plans.UnwindCollection(SingleRow()(solved), IdName("x"), listLiteral)(solved)
    val plan = ProduceResult(List("x"), unwind)

    // when
    val compiled = compileAndExecute(plan)

    // then
    val result = getResult(compiled, "x")
    result.toList should equal(List(Map("x" -> 1.0), Map("x" -> 2.0), Map("x" -> 3.0)))
  }

  test("unwind list of booleans") { // UNWIND [true, false] as x RETURN x
    // given
    val listLiteral = ListLiteral(Seq(True()(pos), False()(pos)))(pos)

    val unwind = plans.UnwindCollection(SingleRow()(solved), IdName("x"), listLiteral)(solved)
    val plan = ProduceResult(List("x"), unwind)

    // when
    val compiled = compileAndExecute(plan)

    // then
    val result = getResult(compiled, "x")
    result.toList should equal(List(Map("x" -> true), Map("x" -> false)))
  }

  test("unwind list of integers with projection") { // UNWIND [1, 2, 3] as x WITH x as y RETURN y
    // given
    val listLiteral = literalIntList(1, 2, 3)

    val unwind = plans.UnwindCollection(SingleRow()(solved), IdName("x"), listLiteral)(solved)
    val projection = plans.Projection(unwind, Map("y" -> varFor("x")))(solved)
    val plan = ProduceResult(List("y"), projection)

    // when
    val compiled = compileAndExecute(plan)

    // then
    val result = getResult(compiled, "y")
    result.toList should equal(List(Map("y" -> 1L), Map("y" -> 2L), Map("y" -> 3L)))
  }

  test("unwind parameter list") { // UNWIND {list} as x RETURN x
    // given
    val parameter = Parameter("list", CTAny)(pos)

    val unwind = plans.UnwindCollection(SingleRow()(solved), IdName("x"), parameter)(solved)
    val plan = ProduceResult(List("x"), unwind)

    // when
    val compiled = compileAndExecute(plan, params = param("list" -> List(1, 2, 3).asJava))

    // then
    val result = getResult(compiled, "x")
    result.toList should equal(List(Map("x" -> 1L), Map("x" -> 2L), Map("x" -> 3L)))
  }

  test("unwind null") { // UNWIND null as x RETURN x
    // given
    val unwind = plans.UnwindCollection(SingleRow()(solved), IdName("x"), Null()(pos))(solved)
    val plan = ProduceResult(List("x"), unwind)

    // when
    val compiled = compileAndExecute(plan)

    // then
    val result = getResult(compiled, "x")
    result.toList should equal(List())
  }

  test("unwind null parameter list") { // UNWIND {list} as x RETURN x
  // given
  val parameter = Parameter("list", CTAny)(pos)

    val unwind = plans.UnwindCollection(SingleRow()(solved), IdName("x"), parameter)(solved)
    val plan = ProduceResult(List("x"), unwind)

    // when
    val compiled = compileAndExecute(plan, params = param("list" -> null))

    // then
    val result = getResult(compiled, "x")
    result.toList should equal(List())
  }

  test("projection of int list") { // WITH [1, 2, 3] as x RETURN x
    // given
    val listLiteral = literalIntList(1, 2, 3)

    val projection = plans.Projection(SingleRow()(solved), Map("x" -> listLiteral))(solved)
    val plan = ProduceResult(List("x"), projection)

    // when
    val compiled = compileAndExecute(plan)

    // then
    val result = getResult(compiled, "x")
    result.toList should equal(List(Map("x" -> List(1L, 2L, 3L))))
  }

  test("projection of double list") { // WITH [1.0, 2.0, 3.0] as x RETURN x
    // given
    val listLiteral = literalFloatList(1.0, 2.0, 3.0)

    val projection = plans.Projection(SingleRow()(solved), Map("x" -> listLiteral))(solved)
    val plan = ProduceResult(List("x"), projection)

    // when
    val compiled = compileAndExecute(plan)

    // then
    val result = getResult(compiled, "x")
    result.toList should equal(List(Map("x" -> List(1.0, 2.0, 3.0))))
  }

  test("projection of boolean list") { // WITH [true, false] as x RETURN x
    // given
    val listLiteral = ListLiteral(Seq(True()(pos), False()(pos)))(pos)

    val projection = plans.Projection(SingleRow()(solved), Map("x" -> listLiteral))(solved)
    val plan = ProduceResult(List("x"), projection)

    // when
    val compiled = compileAndExecute(plan)

    // then
    val result = getResult(compiled, "x")
    result.toList should equal(List(Map("x" -> List(true, false))))
  }

  test("count with non-nullable literal") {
    when(semanticTable.resolvedPropertyKeyNames).thenReturn(mutable.Map.empty[String, PropertyKeyId])
    val scan = AllNodesScan(IdName("a"), Set.empty)(solved)
    val ns: Namespace = Namespace(List())(pos)
    val count: FunctionName = FunctionName("count")(pos)

    val property = literalInt(42) // <== This cannot be nullable

    val invocation: FunctionInvocation = FunctionInvocation(ns, count, distinct = false, Vector(property))(pos)
    val aggregation = Aggregation(scan, Map("a" -> ast.Variable("a")(pos)), Map("c" -> invocation))(solved)
    val plan = ProduceResult(List("c"), aggregation)

    //when
    val compiled = compileAndExecute(plan)

    val result = getResult(compiled, "c")

    //then
    result.toList should equal(List(
      Map("c" -> 1),
      Map("c" -> 1),
      Map("c" -> 1),
      Map("c" -> 1),
      Map("c" -> 1),
      Map("c" -> 1),
      Map("c" -> 1),
      Map("c" -> 1),
      Map("c" -> 1))
    )
  }

  test("count distinct with non-nullable literal") {
    when(semanticTable.resolvedPropertyKeyNames).thenReturn(mutable.Map.empty[String, PropertyKeyId])
    val scan = AllNodesScan(IdName("a"), Set.empty)(solved)
    val ns: Namespace = Namespace(List())(pos)
    val count: FunctionName = FunctionName("count")(pos)

    val property = literalInt(42) // <== This cannot be nullable

    val invocation: FunctionInvocation = FunctionInvocation(ns, count, distinct = true, Vector(property))(pos)
    val aggregation = Aggregation(scan, Map("a" -> ast.Variable("a")(pos)), Map("c" -> invocation))(solved)
    val plan = ProduceResult(List("c"), aggregation)

    //when
    val compiled = compileAndExecute(plan)

    val result = getResult(compiled, "c")

    //then
    result.toList should equal(List(
      Map("c" -> 1),
      Map("c" -> 1),
      Map("c" -> 1),
      Map("c" -> 1),
      Map("c" -> 1),
      Map("c" -> 1),
      Map("c" -> 1),
      Map("c" -> 1),
      Map("c" -> 1))
    )
  }

  test("sort projection with list of integers") {
    /*
    UNWIND [3,1,2,4] as x
    WITH x AS a, x AS b, x AS c, x AS d
    RETURN b, c
    ORDER BY a, b
    */
    // given
    val listLiteral = literalIntList(3, 1, 2, 4)

    val unwind = plans.UnwindCollection(SingleRow()(solved), IdName("x"), listLiteral)(solved)
    val projection = plans.Projection(unwind,
      Map("a" -> varFor("x"),
        "b" -> varFor("x"),
        "c" -> varFor("x"),
        "d" -> varFor("x")))(solved)
    val orderBy = Sort(projection, List(Ascending(IdName("a")), Descending(IdName("b"))))(solved)
    val plan = ProduceResult(List("b", "c"), orderBy)

    // when
    val compiled = compileAndExecute(plan)

    // then
    val result = getResult(compiled, "b", "c")
    result.toList should equal(List(
      Map("b" -> 1L,"c" -> 1L),
      Map("b" -> 2L,"c" -> 2L),
      Map("b" -> 3L,"c" -> 3L),
      Map("b" -> 4L,"c" -> 4L)))
  }

  test("sort projection with list of floats") {
    /*
    UNWIND [3.0,1.0,2.0,4.0] as x
    WITH x, x AS `  FRESHID666`
    RETURN x
    ORDER BY `  FRESHID666`
    */
    // given
    val listLiteral = literalFloatList(3.0, 1.0, 2.0, 4.0)
    val unwind = plans.UnwindCollection(SingleRow()(solved), IdName("x"), listLiteral)(solved)
    val projection = plans.Projection(unwind, Map("x" -> varFor("x"), "  FRESHID666" -> varFor("x")))(solved)
    val orderBy = Sort(projection, List(Ascending(IdName("  FRESHID666"))))(solved)
    val plan = ProduceResult(List("x"), orderBy)

    // when
    val compiled = compileAndExecute(plan)

    // then
    val result = getResult(compiled, "x")
    result.toList should equal(List(
      Map("x" -> 1.0),
      Map("x" -> 2.0),
      Map("x" -> 3.0),
      Map("x" -> 4.0)))
  }

  test("sort projection with list of booleans") {
    /*
    UNWIND [true, false] as x
    WITH x AS a
    RETURN a
    ORDER BY a
    */
    // given
    val listLiteral = ListLiteral(Seq(True()(pos), False()(pos)))(pos)

    val unwind = plans.UnwindCollection(SingleRow()(solved), IdName("x"), listLiteral)(solved)
    val projection = plans.Projection(unwind, Map("a" -> varFor("x")))(solved)
    val orderBy = Sort(projection, List(Ascending(IdName("a"))))(solved)
    val plan = ProduceResult(List("a"), orderBy)

    // when
    val compiled = compileAndExecute(plan)

    // then
    val result = getResult(compiled, "a")
    result.toList should equal(List(
      Map("a" -> false),
      Map("a" -> true)))
  }

  test("sort projection with list of integer maps") {
    /*
    UNWIND [
            {a:3, b:3, c:3, d:3},
            {a:1, b:1, c:1, d:1},
            {a:2, b:2, c:2, d:2},
            {a:4, b:4, c:4, d:4}
           ] as x
    WITH x.a AS a, x.b AS b, x.c AS c, x.d AS d
    RETURN b, c
    ORDER BY a, b
    */
    // given
    val listLiteral = ListLiteral(Seq(
      literalIntMap(("a", 3), ("b", 3), ("c", 3), ("d", 3)),
      literalIntMap(("a", 1), ("b", 1), ("c", 1), ("d", 1)),
      literalIntMap(("a", 1), ("b", 2), ("c", 2), ("d", 2)),
      literalIntMap(("a", 4), ("b", 4), ("c", 4), ("d", 4))
    ))(pos)

    val unwind = plans.UnwindCollection(SingleRow()(solved), IdName("x"), listLiteral)(solved)
    val projection = plans.Projection(unwind, Map("a" -> prop("x", "a"), "b" -> prop("x", "b"), "c" -> prop("x", "c"), "d" -> prop("x", "d")))(solved)
    val orderBy = Sort(projection, List(Ascending(IdName("a")), Descending(IdName("b"))))(solved)
    val plan = ProduceResult(List("b", "c"), orderBy)

    // when
    val compiled = compileAndExecute(plan)

    // then
    val result = getResult(compiled, "b", "c")
    result.toList should equal(List(
      Map("b" -> 2L,"c" -> 2L), // Because we sort Descending on b
      Map("b" -> 1L,"c" -> 1L),
      Map("b" -> 3L,"c" -> 3L),
      Map("b" -> 4L,"c" -> 4L)))
  }

  test("sort and limit projection with list of integers (should use top)") {
    /*
    UNWIND [3,1,2,4] as x
    WITH x AS a, x AS b, x AS c, x AS d
    RETURN b, c
    ORDER BY a, b
    LIMIT 2
    */
    // given
    val listLiteral = literalIntList(3, 1, 2, 4)

    val unwind = plans.UnwindCollection(SingleRow()(solved), IdName("x"), listLiteral)(solved)
    val projection = plans.Projection(unwind,
      Map("a" -> varFor("x"),
        "b" -> varFor("x"),
        "c" -> varFor("x"),
        "d" -> varFor("x")))(solved)
    val orderBy = Sort(projection, List(Ascending(IdName("a")), Descending(IdName("b"))))(solved)
    val limit = plans.Limit(orderBy, literalInt(2), DoNotIncludeTies)(solved)
    val plan = ProduceResult(List("b", "c"), limit)

    // when
    val compiled = compileAndExecute(plan)

    // then
    val result = getResult(compiled, "b", "c")
    result.toList should equal(List(
      Map("b" -> 1L,"c" -> 1L),
      Map("b" -> 2L,"c" -> 2L)))
  }

  test("sort with negative limit from parameter (should use top)") {
    /* {limit -> -1}
    UNWIND [3,1,2,4] as x
    WITH x as a
    RETURN a
    ORDER BY a
    LIMIT {limit}
    */
    // given
    val listLiteral = literalIntList(3, 1, 2, 4)

    val unwind = plans.UnwindCollection(SingleRow()(solved), IdName("x"), listLiteral)(solved)
    val projection = plans.Projection(unwind, Map("a" -> varFor("x")))(solved)
    val orderBy = Sort(projection, List(Ascending(IdName("a"))))(solved)
    val limit = plans.Limit(orderBy, Parameter("limit", CTAny)(pos), DoNotIncludeTies)(solved)
    val plan = ProduceResult(List("a"), limit)

    // when
    val compiled = compileAndExecute(plan, param("limit" -> Long.box(-1L)))

    // then
    val result = getResult(compiled, "a")
    result.toList should equal(List.empty)
  }

  test("negative limit from parameter") {
    /* {limit -> -1}
    UNWIND [1,2,3] as x
    RETURN x
    LIMIT {limit}
    */
    // given
    val listLiteral = literalIntList(1, 2, 3)

    val unwind = plans.UnwindCollection(SingleRow()(solved), IdName("x"), listLiteral)(solved)
    val limit = plans.Limit(unwind, Parameter("limit", CTAny)(pos), DoNotIncludeTies)(solved)
    val plan = ProduceResult(List("x"), limit)

    // when
    val compiled = compileAndExecute(plan, param("limit" -> Long.box(-1L)))

    // then
    val result = getResult(compiled, "x")
    result.toList should equal(List.empty)
  }

  import JavaConverters._
  private def param(values: (String,AnyRef)*): MapValue = ValueUtils.asMapValue(values.toMap.asJava)

  private def compile(plan: LogicalPlan) = {
    plan.assignIds()
    generator.generate(plan, newMockedPlanContext, semanticTable, CostBasedPlannerName.default)
  }

  private def compileAndExecute(plan: LogicalPlan, params: MapValue = EMPTY_MAP, taskCloser: TaskCloser = new TaskCloser) = {
    compile(plan).executionResultBuilder(queryContext, NormalMode, tracer(NormalMode, queryContext), params, taskCloser)
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

  private val allNodes = IndexedSeq(aNode, bNode, cNode, dNode, eNode, fNode, gNode, hNode, iNode)
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

  import JavaConverters._
  private def toObjectConverter(a: AnyRef): AnyRef = a match {
    case Values.NO_VALUE => null
    case n: NodeValue => allNodes(n.id().asInstanceOf[Int])
    case r: EdgeValue => relMap(r.id()).relationship
    case s: TextValue => s.stringValue()
    case b: BooleanValue => Boolean.box(b.booleanValue())
    case f: FloatingPointValue => Double.box(f.doubleValue())
    case i: IntegralValue => Long.box(i.longValue())
    case l: ListValue =>
      val list = new util.ArrayList[AnyRef]
      l.iterator().asScala.foreach(a => list.add(toObjectConverter(a)))
      list
    case m: MapValue =>
      val map = new util.HashMap[String, AnyRef]()
      m.foreach(new BiConsumer[String, AnyValue] {
        override def accept(t: String, u: AnyValue): Unit = map.put(t, toObjectConverter(u))
      })
      map
  }

  private val queryContext = mock[QueryContext]
  private val transactionalContext = mock[TransactionalContextWrapper]
  private val ro = mock[ReadOperations]
  when(queryContext.transactionalContext).thenReturn(transactionalContext)
  when(queryContext.asObject(any())).thenAnswer(new Answer[AnyRef] {
    override def answer(invocationOnMock: InvocationOnMock): AnyRef = toObjectConverter(invocationOnMock.getArguments()(0))
  })
  when(transactionalContext.readOperations).thenReturn(ro)
  when(queryContext.entityAccessor).thenReturn(nodeManager.asInstanceOf[queryContext.EntityAccessor])
  when(ro.nodeGetProperty(anyLong(), anyInt())).thenAnswer(new Answer[Value] {
    override def answer(invocationOnMock: InvocationOnMock): Value = {
      val id = invocationOnMock.getArguments()(0).asInstanceOf[Long]
      if (id < 3) Values.stringValue("value")
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
  when(ro.nodeGetRelationships(anyLong(), any[Direction], any[Array[Int]]())).thenAnswer(new Answer[PrimitiveLongIterator] {
    override def answer(invocationOnMock: InvocationOnMock): PrimitiveLongIterator = {
      val arguments = invocationOnMock.getArguments
      val node = arguments(0).asInstanceOf[Long].toInt
      val dir = arguments(1).asInstanceOf[Direction]
      val types = arguments(2).asInstanceOf[Array[Int]].toSet
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
    override def answer(invocationOnMock: InvocationOnMock): Boolean = {
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

  private def mockNode(id: Long, name: String): NodeProxy = {
    val node = mock[NodeProxy]
    when(node.getId).thenReturn(id)
    when(node.toString).thenReturn(name)
    when(node.getLabels).thenReturn(Iterable.empty[Label].asJava)
    when(node.getAllProperties).thenReturn(Map.empty[String, AnyRef].asJava)
    node
  }

  private def mockRelationship(relationshipData: RelationshipData) = {
    val rel = mock[RelationshipProxy]
    val toStringValue = relationshipData.toString
    when(rel.getId).thenReturn(relationshipData.id)
    when(rel.getStartNode).thenReturn(relationshipData.from)
    when(rel.getEndNode).thenReturn(relationshipData.to)
    when(rel.toString).thenReturn(toStringValue)
    when(rel.getType).thenReturn(org.neo4j.graphdb.RelationshipType.withName("R"))
    when(rel.getAllProperties). thenReturn(Map.empty[String, AnyRef].asJava)
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

    plan.accept(new ResultVisitor[RuntimeException]() {
      override def visit(element: ResultRow): Boolean = {
        res += columns.map(col => col -> element.getNode(col)).toMap
        true
      }
    })
    res.result()
  }

  private def getResult(plan: InternalExecutionResult, columns: String*) = {
    val res = Seq.newBuilder[Map[String, Any]]

    plan.accept(new ResultVisitor[RuntimeException]() {
      override def visit(element: ResultRow): Boolean = {
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
