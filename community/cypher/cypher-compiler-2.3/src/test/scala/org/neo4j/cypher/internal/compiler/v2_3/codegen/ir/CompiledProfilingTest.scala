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

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.neo4j.collection.primitive.PrimitiveLongIterator
import org.neo4j.cypher.internal.compiler.v2_3.ProfileMode
import org.neo4j.cypher.internal.compiler.v2_3.ast.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.compiler.v2_3.codegen.profiling.ProfilingTracer
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription.Arguments.{DbHits, Rows}
import org.neo4j.cypher.internal.compiler.v2_3.planDescription._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.Cardinality
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_3.planner.{CardinalityEstimation, PlannerQuery}
import org.neo4j.cypher.internal.compiler.v2_3.test_helpers.CypherFunSuite
import org.neo4j.function.Supplier
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.{GraphDatabaseService, Node}
import org.neo4j.kernel.api._
import org.neo4j.test.ImpermanentGraphDatabase

class CompiledProfilingTest extends CypherFunSuite with CodeGenSugar {

  test("should count db hits and rows") {
    // given
    val id1 = new Id()
    val id2 = new Id()

    val projectNode = expressions.Node("name")
    val compiled = compile(WhileLoop("name",
      ScanAllNodes("OP1"),
      Project("X", List(projectNode),
      AcceptVisitor("OP2", Map("n" -> projectNode)))))

    val statement = mock[Statement]
    val readOps = mock[ReadOperations]
    val db = mock[GraphDatabaseService]
    when(db.getNodeById(anyLong())).thenReturn(mock[Node])
    when(statement.readOperations()).thenReturn(readOps)
    when(readOps.nodesGetAll()).thenReturn(new PrimitiveLongIterator {
      private var counter = 0

      override def next(): Long = counter

      override def hasNext: Boolean = {
        counter += 1
        counter < 3
      }
    })

    val supplier = new Supplier[InternalPlanDescription] {
      override def get(): InternalPlanDescription =
        PlanDescriptionImpl(id2, "accept", SingleChild(PlanDescriptionImpl(id1, "scanallnodes", NoChildren, Seq.empty, Set.empty)), Seq.empty, Set.empty)
    }

    insertStatic(compiled, "OP1" -> id1, "OP2" -> id2)

    // when
    val tracer = new ProfilingTracer()
    newInstance(compiled, statement = statement, graphdb = db, supplier = supplier, queryExecutionTracer = tracer).size

    // then
    tracer.dbHitsOf(id1) should equal(3)
    tracer.rowsOf(id2) should equal(2)
  }

  def single[T](seq: Seq[T]): T = {
    seq.size should equal(1)
    seq.head
  }

  test("should profile hash join") {
    //given
    val graphDb = new ImpermanentGraphDatabase()
    val tx = graphDb.beginTx()
    graphDb.createNode()
    graphDb.createNode()
    tx.success()
    tx.close()

    val solved = CardinalityEstimation.lift(PlannerQuery.empty, Cardinality(1))
    val lhs = AllNodesScan(IdName("a"), Set.empty)(solved)
    val rhs = AllNodesScan(IdName("a"), Set.empty)(solved)
    val join = NodeHashJoin(Set(IdName("a")), lhs, rhs)(solved)
    val projection = Projection(join, Map("foo" -> SignedDecimalIntegerLiteral("1")(null)))(solved)
    val plan = ProduceResult(List.empty, List.empty, List("foo"), projection)

    // when
    val result = compileAndExecute(plan, graphDb, mode = ProfileMode)
    val description = result.executionPlanDescription()

    // then
    val hashJoin = single(description.find("NodeHashJoin"))
    hashJoin.arguments should contain(DbHits(0))
    hashJoin.arguments should contain(Rows(2))
  }

  test("should generate operator id fields") {
    // given
    val instruction = WhileLoop("name",
      ScanAllNodes("foo"),
      AcceptVisitor("bar", Map.empty))

    // when
    val clazz = compile(instruction)

    // then
    clazz.getDeclaredField("foo").getType should equal(classOf[Id])
    clazz.getDeclaredField("bar").getType should equal(classOf[Id])
  }
}
