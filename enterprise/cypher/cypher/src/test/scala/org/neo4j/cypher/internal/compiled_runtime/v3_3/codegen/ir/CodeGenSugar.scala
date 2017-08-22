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
package org.neo4j.cypher.internal.compiled_runtime.v3_3.codegen.ir

import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger

import org.mockito.Mockito._
import org.neo4j.cypher.internal.InternalExecutionResult
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.compiled.ExecutionPlanBuilder.tracer
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.compiled.codegen._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.compiled.codegen.ir.Instruction
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.compiled.CompiledExecutionResult
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.compiled.CompiledPlan
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.executionplan.Provider
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.Id
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionMode
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.NormalMode
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.TaskCloser
import org.neo4j.cypher.internal.compiler.v3_3.CostBasedPlannerName
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v3_3.spi._
import org.neo4j.cypher.internal.frontend.v3_3.SemanticTable
import org.neo4j.cypher.internal.spi.v3_3.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.spi.v3_3.codegen.GeneratedQueryStructure
import org.neo4j.cypher.internal.spi.v3_3.QueryContext
import org.neo4j.cypher.internal.spi.v3_3.TransactionBoundQueryContext
import org.neo4j.cypher.internal.spi.v3_3.TransactionalContextWrapper
import org.neo4j.cypher.internal.v3_3.codegen.QueryExecutionTracer
import org.neo4j.cypher.internal.v3_3.executionplan.GeneratedQuery
import org.neo4j.cypher.internal.v3_3.executionplan.GeneratedQueryExecution
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Result.ResultRow
import org.neo4j.graphdb.Result.ResultVisitor
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.security.AnonymousContext
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.api.Statement
import org.neo4j.kernel.impl.coreapi.PropertyContainerLocker
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory
import org.neo4j.kernel.impl.query.clientconnection.ClientConnectionInfo
import org.neo4j.time.Clocks
import org.scalatest.mock.MockitoSugar

import scala.collection.JavaConversions

trait CodeGenSugar extends MockitoSugar {

  private val semanticTable = mock[SemanticTable]

  def compile(plan: LogicalPlan): CompiledPlan = {
    val statistics: GraphStatistics = mock[GraphStatistics]
    val context = mock[PlanContext]
    doReturn(statistics).when(context).statistics
    new CodeGenerator(GeneratedQueryStructure, Clocks.systemClock())
      .generate(plan, context, semanticTable, CostBasedPlannerName.default)
  }

  def compileAndExecute(plan: LogicalPlan, graphDb: GraphDatabaseQueryService, mode: ExecutionMode = NormalMode) = {
    executeCompiled(compile(plan), graphDb, mode)
  }

  def executeCompiled(plan: CompiledPlan,
                      graphDb: GraphDatabaseQueryService,
                      mode: ExecutionMode = NormalMode): InternalExecutionResult = {
    val tx = graphDb.beginTransaction(KernelTransaction.Type.explicit, AnonymousContext.read())
    try {
      val locker: PropertyContainerLocker = new PropertyContainerLocker
      val contextFactory = Neo4jTransactionalContextFactory.create(graphDb, locker)
      val transactionalContext = TransactionalContextWrapper(
        contextFactory.newContext(ClientConnectionInfo.EMBEDDED_CONNECTION,
                                  tx,
                                  "no query text exists for this test",
                                  Collections.emptyMap()))
      val queryContext = new TransactionBoundQueryContext(transactionalContext)(mock[IndexSearchMonitor])
      val result = plan
        .executionResultBuilder(queryContext, mode, tracer(mode, queryContext), Map.empty, new TaskCloser)
      tx.success()
      result.size
      result
    } finally {
      tx.close()
    }
  }

  def evaluate(instructions: Seq[Instruction],
               qtx: QueryContext = mockQueryContext(),
               columns: Seq[String] = Seq.empty,
               params: Map[String, AnyRef] = Map.empty,
               operatorIds: Map[String, Id] = Map.empty): List[Map[String, Object]] = {
    val clazz = compile(instructions, columns, operatorIds)
    val result = newInstance(clazz, queryContext = qtx, params = params)
    evaluate(result)
  }

  def evaluate(result: InternalExecutionResult): List[Map[String, Object]] = {
    var rows = List.empty[Map[String, Object]]
    val columns: List[String] = result.columns
    result.accept(new ResultVisitor[RuntimeException] {
      override def visit(row: ResultRow): Boolean = {
        rows = rows :+ columns.map(key => (key, row.get(key))).toMap
        true
      }
    })
    rows
  }

  def codeGenConfiguration = CodeGenConfiguration(mode = ByteCodeMode)

  def compile(instructions: Seq[Instruction],
              columns: Seq[String],
              operatorIds: Map[String, Id] = Map.empty): GeneratedQuery = {
    //In reality the same namer should be used for construction Instruction as in generating code
    //these tests separate the concerns so we give this namer non-standard prefixes
    CodeGenerator
      .generateCode(GeneratedQueryStructure)(instructions, operatorIds, columns, codeGenConfiguration)(
        new CodeGenContext(
          new SemanticTable(),
          Map.empty,
          columns.indices.map(i => columns(i) -> i).toMap,
          new Namer(new AtomicInteger(0), varPrefix = "TEST_VAR", methodPrefix = "TEST_METHOD")
        ))
      .query
  }

  def newInstance(clazz: GeneratedQuery,
                  taskCloser: TaskCloser = new TaskCloser,
                  queryContext: QueryContext = mockQueryContext(),
                  graphdb: GraphDatabaseService = null,
                  executionMode: ExecutionMode = null,
                  provider: Provider[InternalPlanDescription] = null,
                  queryExecutionTracer: QueryExecutionTracer = QueryExecutionTracer.NONE,
                  params: Map[String, AnyRef] = Map.empty): InternalExecutionResult = {
    val generated = clazz.execute(taskCloser,
                                  queryContext,
                                  executionMode,
                                  provider,
                                  queryExecutionTracer,
                                  JavaConversions.mapAsJavaMap(params))
    new CompiledExecutionResult(taskCloser, queryContext, generated, provider)
  }

  def insertStatic(clazz: Class[GeneratedQueryExecution], mappings: (String, Id)*) = mappings.foreach {
    case (name, id) => setStaticField(clazz, name, id)
  }

  private def mockQueryContext() = {
    val qc = mock[QueryContext]
    val transactionalContext = mock[TransactionalContextWrapper]
    val statement = mock[Statement]
    when(qc.transactionalContext).thenReturn(transactionalContext)
    when(transactionalContext.statement).thenReturn(statement)

    qc
  }
}
