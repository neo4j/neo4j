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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.compiled.codegen

import java.time.Clock
import java.util

import org.neo4j.cypher.internal.InternalExecutionResult
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.compiled.ExecutionPlanBuilder.DescriptionProvider
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.compiled.codegen.ir._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.compiled.codegen.spi.CodeStructure
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.compiled.codegen.spi.CodeStructureResult
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.compiled.CompiledExecutionResult
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.compiled.CompiledPlan
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.compiled.RunnablePlan
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.executionplan.PlanFingerprint
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.executionplan.Provider
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.Id
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.LogicalPlan2PlanDescription
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.LogicalPlanIdentificationBuilder
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionMode
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.TaskCloser
import org.neo4j.cypher.internal.compiler.v3_3.planner.CantCompileQueryException
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans.ProduceResult
import org.neo4j.cypher.internal.compiler.v3_3.spi.InstrumentedGraphStatistics
import org.neo4j.cypher.internal.compiler.v3_3.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v3_3.helpers.Eagerly
import org.neo4j.cypher.internal.frontend.v3_3.PlannerName
import org.neo4j.cypher.internal.frontend.v3_3.SemanticTable
import org.neo4j.cypher.internal.spi.v3_3.QueryContext
import org.neo4j.cypher.internal.v3_3.codegen.QueryExecutionTracer
import org.neo4j.cypher.internal.v3_3.executionplan.GeneratedQuery
import org.neo4j.cypher.internal.v3_3.executionplan.GeneratedQueryExecution

class CodeGenerator(val structure: CodeStructure[GeneratedQuery],
                    clock: Clock,
                    conf: CodeGenConfiguration = CodeGenConfiguration()) {

  import CodeGenerator.generateCode

  type PlanDescriptionProvider =
    (InternalPlanDescription) => (Provider[InternalPlanDescription], Option[QueryExecutionTracer])

  def generate(plan: LogicalPlan,
               planContext: PlanContext,
               semanticTable: SemanticTable,
               plannerName: PlannerName): CompiledPlan = {
    plan match {
      case res: ProduceResult =>
        val idMap = LogicalPlanIdentificationBuilder(plan)

        val query: CodeStructureResult[GeneratedQuery] = try {
          generateQuery(plan, semanticTable, idMap, res.columns, conf)
        } catch {
          case e: CantCompileQueryException => throw e
          case e: Exception                 => throw new CantCompileQueryException(cause = e)
        }

        val fp = planContext.statistics match {
          case igs: InstrumentedGraphStatistics =>
            Some(PlanFingerprint(clock.millis(), planContext.txIdProvider(), igs.snapshot.freeze))
          case _ =>
            None
        }

        val descriptionTree = LogicalPlan2PlanDescription(plan, idMap, plannerName)
        val description: InternalPlanDescription = query.code.foldLeft(descriptionTree) {
          case (descriptionRoot, code) => descriptionRoot.addArgument(code)
        }

        val builder = new RunnablePlan {
          def apply(queryContext: QueryContext,
                    execMode: ExecutionMode,
                    descriptionProvider: DescriptionProvider,
                    params: Map[String, Any],
                    closer: TaskCloser): InternalExecutionResult = {
            val (provider, tracer) = descriptionProvider(description)
            val execution: GeneratedQueryExecution = query.query.execute(closer,
                                                                         queryContext,
                                                                         execMode,
                                                                         provider,
                                                                         tracer.getOrElse(QueryExecutionTracer.NONE),
                                                                         asJavaHashMap(params))
            new CompiledExecutionResult(closer, queryContext, execution, provider)
          }
        }

        CompiledPlan(updating = false, None, fp, plannerName, description, res.columns, builder, plan.indexUsage)

      case _ => throw new CantCompileQueryException("Can only compile plans with ProduceResult on top")
    }
  }

  private def generateQuery(plan: LogicalPlan,
                            semantics: SemanticTable,
                            ids: Map[LogicalPlan, Id],
                            columns: Seq[String],
                            conf: CodeGenConfiguration): CodeStructureResult[GeneratedQuery] = {
    import LogicalPlanConverter._
    val lookup            = columns.indices.map(i => columns(i) -> i).toMap
    implicit val context  = new CodeGenContext(semantics, ids, lookup)
    val (_, instructions) = asCodeGenPlan(plan).produce(context)
    generateCode(structure)(instructions, context.operatorIds.map {
      case (id: Id, field: String) => field -> id
    }.toMap, columns, conf)
  }

  private def asJavaHashMap(params: scala.collection.Map[String, Any]) = {
    val jMap = new util.HashMap[String, Object]()
    params.foreach {
      case (key, value) => jMap.put(key, javaValue(value))
    }
    jMap
  }

  import scala.collection.JavaConverters._
  private def javaValue(value: Any): Object = value match {
    case null                             => null
    case iter: Seq[_]                     => iter.map(javaValue).asJava
    case iter: scala.collection.Map[_, _] => Eagerly.immutableMapValues(iter, javaValue).asJava
    case x: Any                           => x.asInstanceOf[AnyRef]
  }
}

object CodeGenerator {
  type SourceSink = Option[(String, String) => Unit]

  def generateCode[T](structure: CodeStructure[T])(
      instructions: Seq[Instruction],
      operatorIds: Map[String, Id],
      columns: Seq[String],
      conf: CodeGenConfiguration)(implicit context: CodeGenContext): CodeStructureResult[T] = {
    structure.generateQuery(Namer.newClassName(), columns, operatorIds, conf) { accept =>
      instructions.foreach(insn => insn.init(accept))
      instructions.foreach(insn => insn.body(accept))
    }
  }
}
