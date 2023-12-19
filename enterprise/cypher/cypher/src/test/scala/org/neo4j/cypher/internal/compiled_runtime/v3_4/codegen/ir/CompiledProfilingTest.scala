/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.cypher.internal.compiled_runtime.v3_4.codegen.ir

import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.Variable
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.expressions.{CodeGenType, NodeProjection}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.{AcceptVisitor, ScanAllNodes, WhileLoop}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan.Provider
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.planner.v3_4.spi.KernelStatisticProvider
import org.neo4j.cypher.internal.runtime.interpreted.TransactionalContextWrapper
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription.Arguments.{DbHits, Rows}
import org.neo4j.cypher.internal.runtime.planDescription.{InternalPlanDescription, NoChildren, PlanDescriptionImpl, SingleChild}
import org.neo4j.cypher.internal.runtime.{ProfileMode, QueryContext, QueryTransactionalContext}
import org.neo4j.cypher.internal.util.v3_4.attribution.Id
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.codegen.profiling.ProfilingTracer
import org.neo4j.cypher.internal.v3_4.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.v3_4.logical.plans
import org.neo4j.cypher.internal.v3_4.logical.plans._
import org.neo4j.internal.kernel.api.CursorFactory
import org.neo4j.internal.kernel.api.Transaction.Type
import org.neo4j.internal.kernel.api.helpers.{StubNodeCursor, StubRead}
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracer
import org.neo4j.kernel.api.security.AnonymousContext
import org.neo4j.kernel.impl.core.{EmbeddedProxySPI, NodeProxy}
import org.neo4j.test.TestGraphDatabaseFactory

class CompiledProfilingTest extends CypherFunSuite with CodeGenSugar {

  test("should count db hits and rows") {
    // given
    val id1 = new Id(0)
    val id2 = new Id(1)

    val variable = Variable("name", CodeGenType.primitiveNode)
    val projectNode = NodeProjection(variable)
    val compiled = compile(Seq(WhileLoop(variable,
      ScanAllNodes("OP1"), AcceptVisitor("OP2", Map("n" -> projectNode)))),
      Seq("n"), Map("OP1" -> id1, "OP2" -> id2, "X" -> Id.INVALID_ID))

    val cursors = mock[CursorFactory]
    val dataRead = new StubRead
    val nodeCursor = new StubNodeCursor
    nodeCursor.withNode(1)
    nodeCursor.withNode(2)
    when(cursors.allocateNodeCursor()).thenReturn(nodeCursor)
    val entityAccessor = mock[EmbeddedProxySPI]
    val queryContext = mock[QueryContext]
    val transactionalContext = mock[TransactionalContextWrapper]
    when(queryContext.transactionalContext).thenReturn(transactionalContext.asInstanceOf[QueryTransactionalContext])
    when(transactionalContext.kernelStatisticProvider).thenReturn(new DelegatingKernelStatisticProvider(new DefaultPageCursorTracer))
    when(transactionalContext.cursors).thenReturn(cursors)
    when(transactionalContext.dataRead).thenReturn(dataRead)
    when(entityAccessor.newNodeProxy(anyLong())).thenReturn(mock[NodeProxy])
    when(queryContext.entityAccessor).thenReturn(entityAccessor)

    val provider = new Provider[InternalPlanDescription] {
      override def get(): InternalPlanDescription =
        PlanDescriptionImpl(id2, "accept", SingleChild(PlanDescriptionImpl(id1, "scanallnodes", NoChildren, Seq.empty, Set.empty)), Seq.empty, Set.empty)
    }

    // when
    val tracer = new ProfilingTracer(transactionalContext.kernelStatisticProvider)
    newInstance(compiled, queryContext = queryContext, provider = provider, queryExecutionTracer = tracer).size

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
    val database = new TestGraphDatabaseFactory().newImpermanentDatabase()
    try {
      val graphDb = new GraphDatabaseCypherService(database)
      val tx = graphDb.beginTransaction(Type.explicit, AnonymousContext.write())
      database.createNode()
      database.createNode()
      tx.success()
      tx.close()

      val lhs = AllNodesScan("a", Set.empty)
      val rhs = AllNodesScan("a", Set.empty)
      val join = NodeHashJoin(Set("a"), lhs, rhs)
      val projection = plans.Projection(join, Map("foo" -> SignedDecimalIntegerLiteral("1")(null)))
      val plan = plans.ProduceResult(projection, List("foo"))

      // when
      val result = compileAndExecute(plan, graphDb, mode = ProfileMode)
      val description = result.executionPlanDescription()

      // then
      val hashJoin = single(description.find("NodeHashJoin"))
      hashJoin.arguments should contain(DbHits(0))
      hashJoin.arguments should contain(Rows(2))
    } finally {
      database.shutdown()
    }
  }

  class DelegatingKernelStatisticProvider(tracer: DefaultPageCursorTracer) extends KernelStatisticProvider {

    override def getPageCacheHits: Long = tracer.hits()

    override def getPageCacheMisses: Long = tracer.faults()
  }
}
