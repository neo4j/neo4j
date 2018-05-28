/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.compiled_runtime.v3_2.codegen

import java.time.Clock
import java.util

import org.neo4j.cypher.internal.compiled_runtime.v3_2.ExecutionPlanBuilder.DescriptionProvider
import org.neo4j.cypher.internal.compiled_runtime.v3_2.codegen.ir._
import org.neo4j.cypher.internal.compiled_runtime.v3_2.codegen.spi.{CodeStructure, CodeStructureResult}
import org.neo4j.cypher.internal.compiled_runtime.v3_2.executionplan.{GeneratedQuery, GeneratedQueryExecution}
import org.neo4j.cypher.internal.compiled_runtime.v3_2.{CompiledExecutionResult, CompiledPlan, RunnablePlan}
import org.neo4j.cypher.internal.compiler.v3_2.executionplan.{PlanFingerprint, _}
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.{Id, InternalPlanDescription}
import org.neo4j.cypher.internal.compiler.v3_2.planner.CantCompileQueryException
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.{LogicalPlan2PlanDescription, LogicalPlanIdentificationBuilder}
import org.neo4j.cypher.internal.compiler.v3_2.spi.{InstrumentedGraphStatistics, PlanContext, QueryContext}
import org.neo4j.cypher.internal.compiler.v3_2.{ExecutionMode, TaskCloser}
import org.neo4j.cypher.internal.frontend.v3_2.helpers.Eagerly
import org.neo4j.cypher.internal.frontend.v3_2.{PlannerName, SemanticTable}

class CodeGenerator(val structure: CodeStructure[GeneratedQuery], clock: Clock, conf: CodeGenConfiguration = CodeGenConfiguration() ) {

  import CodeGenerator.generateCode

  type PlanDescriptionProvider =
          (InternalPlanDescription) => (Provider[InternalPlanDescription], Option[QueryExecutionTracer])

  def generate(plan: LogicalPlan, planContext: PlanContext, semanticTable: SemanticTable, plannerName: PlannerName): CompiledPlan = {
    plan match {
      case res: ProduceResult =>
        val idMap = LogicalPlanIdentificationBuilder(plan)

        val query: CodeStructureResult[GeneratedQuery] = try {
          generateQuery(plan, semanticTable, idMap, res.columns, conf)
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

        val descriptionTree = LogicalPlan2PlanDescription(plan, idMap)
        val description: InternalPlanDescription = query.code.foldLeft(descriptionTree) {
          case (descriptionRoot, code) => descriptionRoot.addArgument(code)
        }

        val builder = new RunnablePlan {
          def apply(queryContext: QueryContext, execMode: ExecutionMode,
                    descriptionProvider: DescriptionProvider, params: Map[String, Any],
                    closer: TaskCloser): InternalExecutionResult = {
            val (provider, tracer) = descriptionProvider(description)
            val execution: GeneratedQueryExecution = query.query.execute(closer, queryContext, execMode,
              provider, tracer.getOrElse(QueryExecutionTracer.NONE), asJavaHashMap(params))
            new CompiledExecutionResult(closer, queryContext, execution, provider)
          }
        }

        CompiledPlan(updating = false, None, fp, plannerName, description, res.columns, builder, plan.indexUsage)

      case _ => throw new CantCompileQueryException("Can only compile plans with ProduceResult on top")
    }
  }

  private def generateQuery(plan: LogicalPlan, semantics: SemanticTable, ids: Map[LogicalPlan, Id],
                            columns: Seq[String], conf: CodeGenConfiguration): CodeStructureResult[GeneratedQuery] = {
    import LogicalPlanConverter._
    implicit val context = new CodeGenContext(semantics, ids)
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
    case null => null
    case iter: Seq[_] => iter.map(javaValue).asJava
    case iter: scala.collection.Map[_, _] => Eagerly.immutableMapValues(iter, javaValue).asJava
    case x: Any => x.asInstanceOf[AnyRef]
  }
}

object CodeGenerator {
  type SourceSink = Option[(String, String) => Unit]

  def generateCode[T](structure: CodeStructure[T])(instructions: Seq[Instruction], operatorIds: Map[String, Id],
                                                   columns: Seq[String], conf: CodeGenConfiguration)(implicit context: CodeGenContext): CodeStructureResult[T] = {
    structure.generateQuery(Namer.newClassName(), columns, operatorIds, conf) { accept =>
      instructions.foreach(insn => insn.init(accept))
      instructions.foreach(insn => insn.body(accept))
    }
  }
}
