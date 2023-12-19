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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen

import java.time.Clock
import java.util

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.CompiledRuntimeName
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.ExecutionPlanBuilder.DescriptionProvider
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.spi.{CodeStructure, CodeStructureResult}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.{CompiledExecutionResult, CompiledPlan, RunnablePlan}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan.{PlanFingerprint, Provider}
import org.neo4j.cypher.internal.compiler.v3_4.planner.CantCompileQueryException
import org.neo4j.cypher.internal.frontend.v3_4.PlannerName
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticTable
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.{Cardinalities, ReadOnlies}
import org.neo4j.cypher.internal.planner.v3_4.spi.{InstrumentedGraphStatistics, PlanContext}
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription.Arguments.{Runtime, RuntimeImpl}
import org.neo4j.cypher.internal.runtime.planDescription.{InternalPlanDescription, LogicalPlan2PlanDescription}
import org.neo4j.cypher.internal.runtime.{ExecutionMode, InternalExecutionResult, QueryContext}
import org.neo4j.cypher.internal.util.v3_4.attribution.Id
import org.neo4j.cypher.internal.util.v3_4.{Eagerly, TaskCloser}
import org.neo4j.cypher.internal.v3_4.codegen.QueryExecutionTracer
import org.neo4j.cypher.internal.v3_4.executionplan.{GeneratedQuery, GeneratedQueryExecution}
import org.neo4j.cypher.internal.v3_4.logical.plans.{LogicalPlan, ProduceResult}
import org.neo4j.values.virtual.MapValue

class CodeGenerator(val structure: CodeStructure[GeneratedQuery], clock: Clock, conf: CodeGenConfiguration = CodeGenConfiguration() ) {

  import CodeGenerator.generateCode

  type PlanDescriptionProvider =
          (InternalPlanDescription) => (Provider[InternalPlanDescription], Option[QueryExecutionTracer])

  def generate(plan: LogicalPlan, planContext: PlanContext, semanticTable: SemanticTable, plannerName: PlannerName, readOnlies: ReadOnlies, cardinalities: Cardinalities): CompiledPlan = {
    plan match {
      case res: ProduceResult =>
        val query: CodeStructureResult[GeneratedQuery] = try {
          generateQuery(plan, semanticTable, res.columns, conf, cardinalities)
        } catch {
          case e: CantCompileQueryException => throw e
          case e: Exception => throw new CantCompileQueryException(cause = e)
        }

        val fp = planContext.statistics match {
          case igs: InstrumentedGraphStatistics =>
            Some(PlanFingerprint(clock.millis(), planContext.txIdProvider(), igs.snapshot.freeze))
          case _ =>
            None
        }

        val description = new Provider[InternalPlanDescription] {
          override def get(): InternalPlanDescription = {
            val d = LogicalPlan2PlanDescription(plan, plannerName, readOnlies, cardinalities)
            query.code.foldLeft(d) {
              case (descriptionRoot, code) => descriptionRoot.addArgument(code)
            }.addArgument(Runtime(CompiledRuntimeName.toTextOutput))
              .addArgument(RuntimeImpl(CompiledRuntimeName.name))
          }
        }

        val builder = new RunnablePlan {
          def apply(queryContext: QueryContext, execMode: ExecutionMode,
                    descriptionProvider: DescriptionProvider, params: MapValue,
                    closer: TaskCloser): InternalExecutionResult = {
            val (provider, tracer) = descriptionProvider(description)
            val execution: GeneratedQueryExecution = query.query.execute(queryContext, execMode, provider,
                                                                         tracer.getOrElse(QueryExecutionTracer.NONE),params)
            closer.addTask(queryContext.resources.close)
            new CompiledExecutionResult(closer, queryContext, execution, provider)
          }
        }

        CompiledPlan(updating = false, None, fp, plannerName, description, res.columns, builder, plan.indexUsage)

      case _ => throw new CantCompileQueryException("Can only compile plans with ProduceResult on top")
    }
  }

  private def generateQuery(plan: LogicalPlan, semantics: SemanticTable,
                            columns: Seq[String], conf: CodeGenConfiguration, cardinalities: Cardinalities): CodeStructureResult[GeneratedQuery] = {
    import LogicalPlanConverter._
    val lookup = columns.indices.map(i => columns(i) -> i).toMap
    implicit val context = new CodeGenContext(semantics, lookup)
    val (_, instructions) = asCodeGenPlan(plan).produce(context, cardinalities)
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
    case null => null
    case iter: Seq[_] => iter.map(javaValue).asJava
    case iter: scala.collection.Map[_, _] => Eagerly.immutableMapValues(iter, javaValue).asJava
    case x: Any => x.asInstanceOf[AnyRef]
  }
}

object CodeGenerator {
  type SourceSink = Option[(String, String) => Unit]

  def generateCode[T](structure: CodeStructure[T])(instructions: Seq[Instruction],
                                                   operatorIds: Map[String, Id],
                                                   columns: Seq[String],
                                                   conf: CodeGenConfiguration)(implicit context: CodeGenContext): CodeStructureResult[T] = {
    structure.generateQuery(Namer.newClassName(), columns, operatorIds, conf) { accept =>
      instructions.foreach(insn => insn.init(accept))
      instructions.foreach(insn => insn.body(accept))
    }
  }
}
