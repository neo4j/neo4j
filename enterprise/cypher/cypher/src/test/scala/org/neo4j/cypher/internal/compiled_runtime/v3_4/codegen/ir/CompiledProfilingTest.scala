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
package org.neo4j.cypher.internal.compiled_runtime.v3_4.codegen.ir

import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.neo4j.collection.primitive.PrimitiveLongIterator
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.ProfileMode
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.Variable
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.expressions.{CodeGenType, NodeProjection}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.{AcceptVisitor, ScanAllNodes, WhileLoop}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan.Provider
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.planDescription.InternalPlanDescription.Arguments.{DbHits, Rows}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.planDescription._
import org.neo4j.cypher.internal.compiler.v3_4.spi.KernelStatisticProvider
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.ir.v3_4.{Cardinality, CardinalityEstimation, IdName, PlannerQuery}
import org.neo4j.cypher.internal.spi.v3_4.{QueryContext, QueryTransactionalContext, TransactionalContextWrapper}
import org.neo4j.cypher.internal.v3_4.codegen.profiling.ProfilingTracer
import org.neo4j.cypher.internal.v3_4.logical.plans._
import org.neo4j.cypher.internal.v3_4.logical.plans
import org.neo4j.cypher.internal.v3_4.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.javacompat.internal.GraphDatabaseCypherService
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracer
import org.neo4j.kernel.api._
import org.neo4j.kernel.api.security.AnonymousContext
import org.neo4j.kernel.impl.core.{NodeManager, NodeProxy}
import org.neo4j.test.TestGraphDatabaseFactory

class CompiledProfilingTest extends CypherFunSuite with CodeGenSugar {

  test("should count db hits and rows") {
    // given
    val id1 = new LogicalPlanId(0)
    val id2 = new LogicalPlanId(1)

    val variable = Variable("name", CodeGenType.primitiveNode)
    val projectNode = NodeProjection(variable)
    val compiled = compile(Seq(WhileLoop(variable,
      ScanAllNodes("OP1"), AcceptVisitor("OP2", Map("n" -> projectNode)))),
      Seq("n"), Map("OP1" -> id1, "OP2" -> id2, "X" -> LogicalPlanId.DEFAULT))

    val readOps = mock[ReadOperations]
    val entityAccessor = mock[NodeManager]
    val queryContext = mock[QueryContext]
    val transactionalContext = mock[TransactionalContextWrapper]
    when(queryContext.transactionalContext).thenReturn(transactionalContext.asInstanceOf[QueryTransactionalContext])
    when(transactionalContext.kernelStatisticProvider).thenReturn(new DelegatingKernelStatisticProvider(new DefaultPageCursorTracer))
    when(transactionalContext.readOperations).thenReturn(readOps)
    when(entityAccessor.newNodeProxyById(anyLong())).thenReturn(mock[NodeProxy])
    when(queryContext.entityAccessor).thenReturn(entityAccessor.asInstanceOf[queryContext.EntityAccessor])
    when(readOps.nodesGetAll()).thenReturn(new PrimitiveLongIterator {
      private var counter = 0

      override def next(): Long = counter

      override def hasNext: Boolean = {
        counter += 1
        counter < 3
      }
    })

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
      val tx = graphDb.beginTransaction(KernelTransaction.Type.explicit, AnonymousContext.write())
      graphDb.createNode()
      graphDb.createNode()
      tx.success()
      tx.close()

      val solved = CardinalityEstimation.lift(PlannerQuery.empty, Cardinality(1))
      val lhs = AllNodesScan(IdName("a"), Set.empty)(solved)
      val rhs = AllNodesScan(IdName("a"), Set.empty)(solved)
      val join = NodeHashJoin(Set(IdName("a")), lhs, rhs)(solved)
      val projection = plans.Projection(join, Map("foo" -> SignedDecimalIntegerLiteral("1")(null)))(solved)
      val plan = plans.ProduceResult(projection, List("foo"))
      plan.assignIds()

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
