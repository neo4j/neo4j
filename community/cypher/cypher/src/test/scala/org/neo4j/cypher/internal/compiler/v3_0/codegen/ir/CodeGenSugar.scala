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
package org.neo4j.cypher.internal.compiler.v3_0.codegen.ir

import java.util.concurrent.atomic.AtomicInteger

import org.mockito.Mockito._
import org.neo4j.cypher.internal.compatibility.EntityAccessorWrapper3_0
import org.neo4j.cypher.internal.compiler.v3_0.codegen.{Namer, _}
import org.neo4j.cypher.internal.compiler.v3_0.executionplan.ExecutionPlanBuilder.tracer
import org.neo4j.cypher.internal.compiler.v3_0.executionplan._
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.{Id, InternalPlanDescription}
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v3_0.spi.{GraphStatistics, PlanContext}
import org.neo4j.cypher.internal.compiler.v3_0.{CostBasedPlannerName, ExecutionMode, NormalMode, TaskCloser}
import org.neo4j.cypher.internal.frontend.v3_0.SemanticTable
import org.neo4j.cypher.internal.spi.v3_0.GeneratedQueryStructure
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Result.{ResultRow, ResultVisitor}
import org.neo4j.helpers.Clock
import org.neo4j.kernel.api.Statement
import org.neo4j.kernel.impl.GraphDatabaseAPI
import org.neo4j.kernel.impl.core.{NodeManager, ThreadToStatementContextBridge}
import org.scalatest.mock.MockitoSugar

import scala.collection.JavaConversions

trait CodeGenSugar extends MockitoSugar {
  private val semanticTable = mock[SemanticTable]

  def compile(plan: LogicalPlan) = {
    val statistics: GraphStatistics = mock[GraphStatistics]
    val context = mock[PlanContext]
    doReturn(statistics).when(context).statistics
    new CodeGenerator(GeneratedQueryStructure).generate(plan, context, Clock.SYSTEM_CLOCK, semanticTable, CostBasedPlannerName.default)
  }

  def compileAndExecute(plan: LogicalPlan,
                        graphDb: GraphDatabaseAPI,
                        mode: ExecutionMode = NormalMode,
                        params: Map[String, AnyRef] = Map.empty,
                        taskCloser: TaskCloser = new TaskCloser) = {
    executeCompiled(compile(plan), graphDb, mode, params, taskCloser)
  }

  def executeCompiled(plan: CompiledPlan,
                      graphDb: GraphDatabaseAPI,
                      mode: ExecutionMode = NormalMode,
                      params: Map[String, AnyRef] = Map.empty,
                      taskCloser: TaskCloser = new TaskCloser): InternalExecutionResult = {
    val tx = graphDb.beginTx()
    try {
      val statement = graphDb.getDependencyResolver.resolveDependency(classOf[ThreadToStatementContextBridge]).get()
      val nodeManager =
        graphDb.asInstanceOf[GraphDatabaseAPI].getDependencyResolver.resolveDependency(classOf[NodeManager])
      val result = plan.executionResultBuilder(statement, new EntityAccessorWrapper3_0(nodeManager), mode,
        tracer(mode), params, taskCloser)
      tx.success()
      result.size
      result
    } finally {
      tx.close()
    }
  }

  def evaluate(instructions: Seq[Instruction],
               stmt: Statement = mock[Statement],
               entityAccessor: EntityAccessor = null,
               columns: Seq[String] = Seq.empty,
               params: Map[String, AnyRef] = Map.empty,
               operatorIds: Map[String, Id] = Map.empty): List[Map[String, Object]] = {
    val clazz = compile(instructions, columns,  operatorIds)
    val result = newInstance(clazz, statement = stmt, entityAccessor = entityAccessor, params = params)
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

  def compile(instructions: Seq[Instruction], columns: Seq[String], operatorIds: Map[String, Id] = Map.empty): GeneratedQuery = {
    //In reality the same namer should be used for construction Instruction as in generating code
    //these tests separate the concerns so we give this namer non-standard prefixes
    CodeGenerator.generateCode(GeneratedQueryStructure)(instructions, operatorIds, columns)(
      new CodeGenContext(new SemanticTable(), Map.empty, new Namer(
        new AtomicInteger(0), varPrefix = "TEST_VAR", methodPrefix = "TEST_METHOD")))
  }

  def newInstance(clazz: GeneratedQuery,
                  taskCloser: TaskCloser = new TaskCloser,
                  statement: Statement = mock[Statement],
                  graphdb: GraphDatabaseService = null,
                  entityAccessor: EntityAccessor = null,
                  executionMode: ExecutionMode = null,
                  provider: Provider[InternalPlanDescription] = null,
                  queryExecutionTracer: QueryExecutionTracer = QueryExecutionTracer.NONE,
                  params: Map[String, AnyRef] = Map.empty): InternalExecutionResult = {
    val generated = clazz.execute(taskCloser, statement, entityAccessor,
      executionMode, provider, queryExecutionTracer, JavaConversions.mapAsJavaMap(params))
    new CompiledExecutionResult(taskCloser, statement, generated, provider)
  }

  def insertStatic(clazz: Class[GeneratedQueryExecution], mappings: (String, Id)*) = mappings.foreach {
    case (name, id) => setStaticField(clazz, name, id)
  }
}
