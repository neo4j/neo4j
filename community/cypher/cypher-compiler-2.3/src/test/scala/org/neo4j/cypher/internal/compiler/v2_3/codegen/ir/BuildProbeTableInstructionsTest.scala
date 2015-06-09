/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.codegen.ir

import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.collection.primitive.PrimitiveLongIterator
import org.neo4j.cypher.internal.compiler.v2_3.codegen.{CodeGenContext, JoinTableMethod, Variable}
import org.neo4j.cypher.internal.compiler.v2_3.planner.SemanticTable
import org.neo4j.cypher.internal.compiler.v2_3.symbols
import org.neo4j.cypher.internal.compiler.v2_3.test_helpers.CypherFunSuite
import org.neo4j.graphdb.{GraphDatabaseService, Node}
import org.neo4j.kernel.api.{ReadOperations, Statement}

import scala.collection.mutable

class BuildProbeTableInstructionsTest extends CypherFunSuite with CodeGenSugar {

  private val tableVarName = "probeTable"
  private val tableKeyVarName = "nodeId"
  private val buildTableMethodName = "buildProbeTable"
  private val probeKeyVarName = "probeKey"
  private val resultRowKey = "resultKey"

  private val db = mock[GraphDatabaseService]
  private val statement = mock[Statement]
  private val readOps = mock[ReadOperations]
  private val allNodeIds = mutable.ArrayBuffer[Long]()

  // used by instructions that generate probe tables
  private implicit val codeGenContext = new CodeGenContext(SemanticTable(), Map.empty)

  when(statement.readOperations()).thenReturn(readOps)
  when(readOps.nodesGetAll()).then(new Answer[PrimitiveLongIterator] {
    def answer(invocation: InvocationOnMock) = allNodeIdsIterator()
  })

  override protected def beforeEach() = allNodeIds.clear()

  test("should generate correct code for simple counting probe table") {
    // Given
    setUpNodeMocks(1, 2, 42)

    val buildInstruction = BuildCountingProbeTable(id = "countingTable",
                                                   name = tableVarName,
                                                   node = Variable(tableKeyVarName, symbols.CTNode))

    // When
    val results = runTest(buildInstruction)

    // Then
    results should have size 3
    val (a :: b :: c :: Nil) = results
    checkNodeResult(1, a)
    checkNodeResult(2, b)
    checkNodeResult(42, c)
  }

  test("should generate correct code for simple recording probe table") {
    // Given
    setUpNodeMocks(42, 4242)

    val buildInstruction = BuildRecordingProbeTable(id = "recordingTable",
                                                    name = tableVarName,
                                                    node = Variable(tableKeyVarName, symbols.CTNode),
                                                    valueSymbols = Map(tableKeyVarName -> Variable(tableKeyVarName, symbols.CTNode)))

    // When
    val results = runTest(buildInstruction)

    // Then
    results should have size 2
    val (a :: b :: Nil) = results
    checkNodeResult(42, a)
    checkNodeResult(4242, b)
  }

  private def setUpNodeMocks(ids: Long*): Unit = {
    ids.foreach { id =>
      val nodeMock = mock[Node]
      when(nodeMock.getId).thenReturn(id)
      when(db.getNodeById(id)).thenReturn(nodeMock)
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

  private def runTest(buildInstruction: BuildProbeTable): List[Map[String, Object]] = {
    val instructions = buildProbeTableWithTwoAllNodeScans(buildInstruction)
    val ids = instructions.flatMap(_.allOperatorIds.map(id => id -> null)).toMap
    evaluate(instructions, statement, db, Map.empty[String, Object], ids)
  }

  private def buildProbeTableWithTwoAllNodeScans(buildInstruction: BuildProbeTable): Seq[Instruction] = {
    val buildWhileLoop = WhileLoop(Variable(tableKeyVarName, symbols.CTNode), ScanAllNodes("scanOp1"), buildInstruction)

    val buildProbeTableMethod = MethodInvocation(operatorId = Set.empty,
                                                 symbol = JoinTableMethod(tableVarName, buildInstruction.tableType),
                                                 methodName = buildTableMethodName,
                                                 statements = Seq(buildWhileLoop))


    val acceptVisitor = AcceptVisitor("visitorOp", Map(resultRowKey -> expressions.Node(Variable(probeKeyVarName, symbols.CTNode))))

    val probeTheTable = GetMatchesFromProbeTable(key = Variable(probeKeyVarName, symbols.CTNode),
                                                 code = buildInstruction.joinData,
                                                 action = acceptVisitor)

    val probeTheTableWhileLoop = WhileLoop(variable = Variable(probeKeyVarName, symbols.CTNode),
                                           producer = ScanAllNodes("scanOp2"),
                                           action = probeTheTable)

    Seq(buildProbeTableMethod, probeTheTableWhileLoop)
  }

}
