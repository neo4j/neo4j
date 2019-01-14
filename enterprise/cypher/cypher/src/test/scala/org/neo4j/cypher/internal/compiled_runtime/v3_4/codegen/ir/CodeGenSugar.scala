/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.compiled_runtime.v3_4.codegen.ir

import java.util.concurrent.atomic.AtomicInteger

import org.mockito.Mockito._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.ExecutionPlanBuilder.tracer
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.Instruction
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.{CompiledExecutionResult, CompiledPlan}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan.Provider
import org.neo4j.cypher.internal.compiler.v3_4.planner.LogicalPlanConstructionTestSupport
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticTable
import org.neo4j.cypher.internal.planner.v3_4.spi.{CostBasedPlannerName, GraphStatistics, PlanContext}
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.runtime.interpreted.{TransactionBoundQueryContext, TransactionalContextWrapper}
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.runtime.{ExecutionMode, InternalExecutionResult, NormalMode, QueryContext}
import org.neo4j.cypher.internal.spi.v3_4.codegen.GeneratedQueryStructure
import org.neo4j.cypher.internal.util.v3_4.TaskCloser
import org.neo4j.cypher.internal.util.v3_4.attribution.Id
import org.neo4j.cypher.internal.v3_4.codegen.QueryExecutionTracer
import org.neo4j.cypher.internal.v3_4.executionplan.{GeneratedQuery, GeneratedQueryExecution}
import org.neo4j.cypher.internal.v3_4.logical.plans.LogicalPlan
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Result.{ResultRow, ResultVisitor}
import org.neo4j.internal.kernel.api.Transaction.Type
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.security.AnonymousContext
import org.neo4j.kernel.impl.coreapi.PropertyContainerLocker
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory
import org.neo4j.kernel.impl.query.clientconnection.ClientConnectionInfo
import org.neo4j.time.Clocks
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues.EMPTY_MAP
import org.scalatest.mock.MockitoSugar

trait CodeGenSugar extends MockitoSugar with LogicalPlanConstructionTestSupport {

  private val semanticTable = mock[SemanticTable]

  def compile(plan: LogicalPlan): CompiledPlan = {
    val statistics: GraphStatistics = mock[GraphStatistics]
    val context = mock[PlanContext]
    doReturn(statistics, Nil: _*).when(context).statistics
    new CodeGenerator(GeneratedQueryStructure, Clocks.systemClock())
      .generate(plan, context, semanticTable, CostBasedPlannerName.default, new StubReadOnlies, new StubCardinalities)
  }

  def compileAndExecute(plan: LogicalPlan,
                        graphDb: GraphDatabaseQueryService,
                        mode: ExecutionMode = NormalMode) = {
    executeCompiled(compile(plan), graphDb, mode)
  }

  def executeCompiled(plan: CompiledPlan,
                      graphDb: GraphDatabaseQueryService,
                      mode: ExecutionMode = NormalMode): InternalExecutionResult = {
    val tx = graphDb.beginTransaction(Type.explicit, AnonymousContext.read())
    var transactionalContext: TransactionalContextWrapper = null
    try {
      val locker: PropertyContainerLocker = new PropertyContainerLocker
      val contextFactory = Neo4jTransactionalContextFactory.create(graphDb, locker)
      transactionalContext = TransactionalContextWrapper(
        contextFactory.newContext(ClientConnectionInfo.EMBEDDED_CONNECTION, tx,
                                  "no query text exists for this test", EMPTY_MAP))
      val queryContext = new TransactionBoundQueryContext(transactionalContext)(mock[IndexSearchMonitor])
      val result = plan
        .executionResultBuilder(queryContext, mode, tracer(mode, queryContext), EMPTY_MAP, new TaskCloser)
      tx.success()
      result.size
      result
    } finally {
      transactionalContext.close(true)
      tx.close()
    }
  }

  def evaluate(instructions: Seq[Instruction],
               qtx: QueryContext = mockQueryContext(),
               columns: Seq[String] = Seq.empty,
               params: MapValue = EMPTY_MAP,
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

  def compile(instructions: Seq[Instruction], columns: Seq[String],
              operatorIds: Map[String, Id] = Map.empty): GeneratedQuery = {
    //In reality the same namer should be used for construction Instruction as in generating code
    //these tests separate the concerns so we give this namer non-standard prefixes
    CodeGenerator.generateCode(GeneratedQueryStructure)(instructions, operatorIds, columns, codeGenConfiguration)(
      new CodeGenContext(new SemanticTable(), columns.indices.map(i => columns(i) -> i).toMap, new Namer(
        new AtomicInteger(0), varPrefix = "TEST_VAR", methodPrefix = "TEST_METHOD"))).query
  }

  def newInstance(clazz: GeneratedQuery,
                  taskCloser: TaskCloser = new TaskCloser,
                  queryContext: QueryContext = mockQueryContext(),
                  graphdb: GraphDatabaseService = null,
                  executionMode: ExecutionMode = null,
                  provider: Provider[InternalPlanDescription] = null,
                  queryExecutionTracer: QueryExecutionTracer = QueryExecutionTracer.NONE,
                  params: MapValue = EMPTY_MAP): InternalExecutionResult = {
    val generated = clazz.execute(queryContext,
                                  executionMode, provider, queryExecutionTracer, params)
    new CompiledExecutionResult(taskCloser, queryContext, generated, provider)
  }

  def insertStatic(clazz: Class[GeneratedQueryExecution], mappings: (String, Id)*) = mappings.foreach {
    case (name, id) => setStaticField(clazz, name, id.asInstanceOf[AnyRef])
  }

  private def mockQueryContext() = {
    val qc = mock[QueryContext]
    val transactionalContext = mock[TransactionalContextWrapper]
    when(qc.transactionalContext).thenReturn(transactionalContext)

    qc
  }
}
