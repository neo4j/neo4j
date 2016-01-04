/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_0.codegen

import java.lang.Boolean.getBoolean
import java.util

import org.neo4j.cypher.internal.compiler.v3_0.codegen.CodeGenerator.SourceSink
import org.neo4j.cypher.internal.compiler.v3_0.codegen.ir._
import org.neo4j.cypher.internal.compiler.v3_0.executionplan.ExecutionPlanBuilder.DescriptionProvider
import org.neo4j.cypher.internal.compiler.v3_0.executionplan.{CompiledPlan, PlanFingerprint, _}
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.InternalPlanDescription.Arguments.SourceCode
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.{Id, InternalPlanDescription}
import org.neo4j.cypher.internal.compiler.v3_0.planner.CantCompileQueryException
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.{LogicalPlan2PlanDescription, LogicalPlanIdentificationBuilder}
import org.neo4j.cypher.internal.compiler.v3_0.spi.{InstrumentedGraphStatistics, PlanContext, QueryContext}
import org.neo4j.cypher.internal.compiler.v3_0.{ExecutionMode, PlannerName, TaskCloser}
import org.neo4j.cypher.internal.frontend.v3_0.SemanticTable
import org.neo4j.cypher.internal.frontend.v3_0.helpers.Eagerly
import org.neo4j.helpers.Clock

class CodeGenerator(val structure: CodeStructure[GeneratedQuery]) {

  import CodeGenerator.generateCode

  type PlanDescriptionProvider =
          (InternalPlanDescription) => (Provider[InternalPlanDescription], Option[QueryExecutionTracer])

  def generate(plan: LogicalPlan, planContext: PlanContext, clock: Clock, semanticTable: SemanticTable, plannerName: PlannerName) = {
    plan match {
      case res: ProduceResult =>
        val idMap = LogicalPlanIdentificationBuilder(plan)

        var sources = Map.empty[String, String]
        val sourceSink: SourceSink = if(getBoolean("org.neo4j.cypher.internal.codegen.IncludeSourcesInPlanDescription")) Some(
          (className:String, sourceCode:String) => { sources = sources.updated(className, sourceCode) }) else None

        val query: GeneratedQuery = generateQuery(plan, semanticTable, idMap, res.columns, sourceSink)

        val fp = planContext.statistics match {
          case igs: InstrumentedGraphStatistics =>
            Some(PlanFingerprint(clock.currentTimeMillis(), planContext.txIdProvider(), igs.snapshot.freeze))
          case _ =>
            None
        }

        val description: InternalPlanDescription = sources.foldLeft(LogicalPlan2PlanDescription(plan, idMap)) {
          case (root, (className, sourceCode)) => root.addArgument(SourceCode(className, sourceCode))
        }

        val builder = new RunnablePlan {
          def apply(queryContext: QueryContext, execMode: ExecutionMode,
                    descriptionProvider: DescriptionProvider, params: Map[String, Any],
                    closer: TaskCloser): InternalExecutionResult = {
            val (provider, tracer) = descriptionProvider(description)
            val execution: GeneratedQueryExecution = query.execute(closer, queryContext, execMode,
              provider, tracer.getOrElse(QueryExecutionTracer.NONE), asJavaHashMap(params))
            new CompiledExecutionResult(closer, queryContext, execution, provider)
          }
        }

        CompiledPlan(updating = false, None, fp, plannerName, description, res.columns, builder)

      case _ => throw new CantCompileQueryException("Can only compile plans with ProduceResult on top")
    }
  }

  private def generateQuery(plan: LogicalPlan, semantics: SemanticTable, ids: Map[LogicalPlan, Id], columns: Seq[String], sources: SourceSink = None): GeneratedQuery = {
    import LogicalPlanConverter._
    implicit val context = new CodeGenContext(semantics, ids)
    val (_, instructions) = asCodeGenPlan(plan).produce(context)
    generateCode(structure)(instructions, context.operatorIds.map {
      case (id: Id, field: String) => field -> id
    }.toMap, columns, sources)
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

  def generateCode[T](structure: CodeStructure[T])(instructions: Seq[Instruction], operatorIds: Map[String, Id], columns: Seq[String], sources:SourceSink=None)(implicit context: CodeGenContext): T = {
    structure.generateQuery(packageName, Namer.newClassName(), columns, operatorIds, sources) { accept =>
      instructions.foreach(insn => insn.init(accept))
      instructions.foreach(insn => insn.body(accept))
    }
  }

  private val packageName = "org.neo4j.cypher.internal.compiler.v3_0.generated"
}
