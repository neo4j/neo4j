/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2.codegen

import java.time.Clock
import java.util

import org.neo4j.cypher.internal.compiler.v3_2.codegen.ir._
import org.neo4j.cypher.internal.compiler.v3_2.codegen.spi.{CodeStructure, CodeStructureResult}
import org.neo4j.cypher.internal.compiler.v3_2.executionplan.ExecutionPlanBuilder.DescriptionProvider
import org.neo4j.cypher.internal.compiler.v3_2.executionplan.{CompiledPlan, PlanFingerprint, _}
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.InternalPlanDescription.Arguments.SourceCode
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.{Id, InternalPlanDescription}
import org.neo4j.cypher.internal.compiler.v3_2.planner.CantCompileQueryException
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.{LogicalPlan2PlanDescription, LogicalPlanIdentificationBuilder}
import org.neo4j.cypher.internal.compiler.v3_2.spi.{InstrumentedGraphStatistics, PlanContext, QueryContext}
import org.neo4j.cypher.internal.compiler.v3_2.{ExecutionMode, PlannerName, TaskCloser}
import org.neo4j.cypher.internal.frontend.v3_2.SemanticTable
import org.neo4j.cypher.internal.frontend.v3_2.helpers.Eagerly

class CodeGenerator(val structure: CodeStructure[GeneratedQuery], clock: Clock, conf: CodeGenConfiguration = CodeGenConfiguration() ) {

  import CodeGenerator.generateCode

  type PlanDescriptionProvider =
          (InternalPlanDescription) => (Provider[InternalPlanDescription], Option[QueryExecutionTracer])

  def generate(plan: LogicalPlan, planContext: PlanContext, semanticTable: SemanticTable, plannerName: PlannerName): CompiledPlan = {
    plan match {
      case res: ProduceResult =>
        val idMap = LogicalPlanIdentificationBuilder(plan)

        val query: CodeStructureResult[GeneratedQuery] = generateQuery(plan, semanticTable, idMap, res.columns, conf)

        val fp = planContext.statistics match {
          case igs: InstrumentedGraphStatistics =>
            Some(PlanFingerprint(clock.millis(), planContext.txIdProvider(), igs.snapshot.freeze))
          case _ =>
            None
        }

        val description: InternalPlanDescription = query.source.foldLeft(LogicalPlan2PlanDescription(plan, idMap)) {
          case (root, (className, sourceCode)) => root.addArgument(SourceCode(className, sourceCode))
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
