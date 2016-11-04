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
package org.neo4j.cypher.internal.compiler.v3_2.codegen.ir

import java.util.concurrent.atomic.AtomicInteger

import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.collection.primitive.PrimitiveLongIterator
import org.neo4j.cypher.internal.compiler.v3_2.codegen.ir.expressions.CodeGenType
import org.neo4j.cypher.internal.compiler.v3_2.codegen.{CodeGenContext, JoinTableMethod, Variable}
import org.neo4j.cypher.internal.compiler.v3_2.spi.QueryContext
import org.neo4j.cypher.internal.frontend.v3_2.SemanticTable
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.spi.v3_2.TransactionalContextWrapper
import org.neo4j.graphdb.Node
import org.neo4j.kernel.api.ReadOperations
import org.neo4j.kernel.impl.core.{NodeManager, NodeProxy}

import scala.collection.mutable

class BuildProbeTableInstructionsTest extends CypherFunSuite with CodeGenSugar {

  private val tableVarName = "probeTable"
  private val buildTableMethodName = "buildProbeTable"
  private val resultRowKey = "resultKey"

  private val entityAccessor = mock[NodeManager]
  private val queryContext = mock[QueryContext]
  private val transactionalContext = mock[TransactionalContextWrapper]
  private val readOps = mock[ReadOperations]
  private val allNodeIds = mutable.ArrayBuffer[Long]()

  // used by instructions that generate probe tables
  private implicit val codeGenContext = new CodeGenContext(SemanticTable(), Map.empty)
  when(queryContext.transactionalContext).thenReturn(transactionalContext)
  when(transactionalContext.readOperations).thenReturn(readOps)
  when(queryContext.entityAccessor).thenReturn(entityAccessor.asInstanceOf[queryContext.EntityAccessor])
  when(readOps.nodesGetAll()).then(new Answer[PrimitiveLongIterator] {
    def answer(invocation: InvocationOnMock) = allNodeIdsIterator()
  })

  override protected def beforeEach() = allNodeIds.clear()

  test("should generate correct code for simple counting probe table") {
    // Given
    setUpNodeMocks(1, 2, 42)
    val nodeVar = "node"
    val nodes = Set(Variable(nodeVar, CodeGenType.primitiveNode))

    val buildInstruction = BuildCountingProbeTable(id = "countingTable",
                                                   name = tableVarName,
                                                   nodes = nodes)

    // When
    val results = runTest(buildInstruction, nodes)

    // Then
    results should have size 3
    val (a :: b :: c :: Nil) = results
    checkNodeResult(1, a)
    checkNodeResult(2, b)
    checkNodeResult(42, c)
  }

  test("should generate correct code for a counting probe table on multiple keys") {
    // Given
    setUpNodeMocks(1, 2)
    val nodes = Set(Variable("node1",CodeGenType.primitiveNode), Variable("node2", CodeGenType.primitiveNode))
    val buildInstruction = BuildCountingProbeTable(id = "countingTable",
                                                   name = tableVarName,
                                                   nodes = nodes)

    // When
    val results = runTest(buildInstruction, nodes)

    // Then
    results should have size 4
    val (a :: b :: c :: d :: Nil) = results
    checkNodeResult(1, a)
    checkNodeResult(1, b)
    checkNodeResult(2, c)
    checkNodeResult(2, d)
  }

  test("should generate correct code for simple recording probe table") {
    // Given
    setUpNodeMocks(42, 4242)
    val nodeVar = "node"
    val nodes = Set(Variable(nodeVar,CodeGenType.primitiveNode))
    val buildInstruction = BuildRecordingProbeTable(id = "recordingTable",
                                                    name = tableVarName,
                                                    nodes = nodes,
                                                    valueSymbols = Map(nodeVar -> Variable(nodeVar, CodeGenType.primitiveNode)))

    // When
    val results = runTest(buildInstruction, nodes)

    // Then
    results should have size 2
    val (a :: b :: Nil) = results
    checkNodeResult(42, a)
    checkNodeResult(4242, b)
  }

  test("should generate correct code for recording probe table on multiple keys") {
    // Given
    setUpNodeMocks(42, 4242)
    val joinNodes = Set(Variable("node1", CodeGenType.primitiveNode), Variable("node2", CodeGenType.primitiveNode))
    val buildInstruction = BuildRecordingProbeTable(id = "recordingTable",
      name = tableVarName,
      nodes = joinNodes,
      valueSymbols = Map("node1" -> Variable("node1", CodeGenType.primitiveNode)))

    // When
    val results = runTest(buildInstruction, joinNodes)

    // Then
    results should have size 4
    val (a :: b :: c:: d :: Nil) = results
    checkNodeResult(42, a)
    checkNodeResult(42, b)
    checkNodeResult(4242, c)
    checkNodeResult(4242, d)
  }

  private def setUpNodeMocks(ids: Long*): Unit = {
    ids.foreach { id =>
      val nodeMock = mock[NodeProxy]
      when(nodeMock.getId).thenReturn(id)
      when(entityAccessor.newNodeProxyById(id)).thenReturn(nodeMock)
      allNodeIds += id
    }
  }

  private def checkNodeResult(id: Long, res: Map[String, Object]): Unit = {
    res.size shouldEqual 1
    val node = res(resultRowKey).asInstanceOf[Node]
    node.getId shouldEqual id
  }

  private def allNodeIdsIterator() = new PrimitiveLongIterator {
    val inner = allNodeIds.iterator

    override def hasNext = inner.hasNext

    override def next() = inner.next()
  }

  private def runTest(buildInstruction: BuildProbeTable, nodes: Set[Variable]): List[Map[String, Object]] = {
    val instructions = buildProbeTableWithTwoAllNodeScans(buildInstruction, nodes)
    val ids = instructions.flatMap(_.allOperatorIds.map(id => id -> null)).toMap
    evaluate(instructions, queryContext, Seq(resultRowKey), Map.empty[String, Object], ids)
  }

  private def buildProbeTableWithTwoAllNodeScans(buildInstruction: BuildProbeTable, nodes: Set[Variable]): Seq[Instruction] = {
    val counter = new AtomicInteger(0)
    val buildWhileLoop = nodes.foldRight[Instruction](buildInstruction){
      case (variable, instruction) => WhileLoop(variable, ScanAllNodes("scanOp" + counter.incrementAndGet()), instruction)
    }

    val buildProbeTableMethod = MethodInvocation(operatorId = Set.empty,
                                                 symbol = JoinTableMethod(tableVarName, buildInstruction.tableType),
                                                 methodName = buildTableMethodName,
                                                 statements = Seq(buildWhileLoop))

    val probeVars = nodes.map(n => n.copy(name = s"probe" + n.name))
    //just put one node in the actual result
    val resultVar = probeVars.head

    val acceptVisitor = AcceptVisitor("visitorOp", Map(resultRowKey -> expressions.NodeProjection(resultVar)))

    val probeTheTable = GetMatchesFromProbeTable(keys = probeVars,
                                                 code = buildInstruction.joinData,
                                                 action = acceptVisitor)

    val probeTheTableWhileLoop = probeVars.foldRight[Instruction](probeTheTable){
      case (variable, instruction) => WhileLoop(variable, ScanAllNodes("scanOp" + counter.incrementAndGet()), instruction)
    }
    Seq(buildProbeTableMethod, probeTheTableWhileLoop)
  }

}
