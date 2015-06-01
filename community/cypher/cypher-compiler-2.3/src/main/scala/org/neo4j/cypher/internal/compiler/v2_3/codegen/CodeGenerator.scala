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
package org.neo4j.cypher.internal.compiler.v2_3.codegen

import java.util

import org.neo4j.cypher.internal.compiler.v2_3.codegen.ir._
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{CompiledPlan, PlanFingerprint, _}
import org.neo4j.cypher.internal.compiler.v2_3.helpers.Eagerly
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.{Id, InternalPlanDescription}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.{LogicalPlan2PlanDescription, LogicalPlanIdentificationBuilder}
import org.neo4j.cypher.internal.compiler.v2_3.planner.{CantCompileQueryException, SemanticTable}
import org.neo4j.cypher.internal.compiler.v2_3.spi.{InstrumentedGraphStatistics, PlanContext}
import org.neo4j.cypher.internal.compiler.v2_3.{ExecutionMode, PlannerName, TaskCloser}
import org.neo4j.function.Supplier
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.helpers.Clock
import org.neo4j.kernel.api.Statement

import scala.collection.{Map, immutable, mutable}

object CodeGenerator {

  def generateClass(instructions: Seq[Instruction])(implicit context: CodeGenContext) = {
    val columns = instructions.flatMap(_.allColumns)
    val opIds = instructions.flatMap(_.allOperatorIds)
    CodeStructure.__TODO__MOVE_IMPLEMENTATION.generateQuery(packageName, Namer.newClassName(),
      columns, opIds) { acceptMethod =>
      instructions.foreach(insn => insn.init(acceptMethod))
      instructions.foreach(insn => insn.body(acceptMethod))
    }
  }

  private val packageName = "org.neo4j.cypher.internal.compiler.v2_3.generated"

  private def createInstructions(plan: LogicalPlan)(implicit context: CodeGenContext): (Seq[Instruction], mutable.Map[Id, String]) = {
    import LogicalPlanConverter._

    val (_, result) = plan.asCodeGenPlan.produce(context)
    (result, context.operatorIds)
  }
}


class CodeGenerator {

  import CodeGenerator.{createInstructions, generateClass}

  import scala.collection.JavaConverters._

  def generate(plan: LogicalPlan, planContext: PlanContext, clock: Clock, semanticTable: SemanticTable, plannerName: PlannerName) = {
    plan match {
      case res: ProduceResult =>
        val idMap = LogicalPlanIdentificationBuilder(plan)
        implicit val context = new CodeGenContext(semanticTable, idMap)
        val (instructions, operatorMap) = createInstructions(plan)
        val clazz = generateClass(instructions)
        operatorMap.foreach {
          case (id, name) => setStaticField(clazz, name, id)
        }

        val fp = planContext.statistics match {
          case igs: InstrumentedGraphStatistics =>
            Some(PlanFingerprint(clock.currentTimeMillis(), planContext.txIdProvider(), igs.snapshot.freeze))
          case _ =>
            None
        }

        val description: InternalPlanDescription = LogicalPlan2PlanDescription(plan, idMap)

        val builder = new RunnablePlan {
          def apply(statement: Statement, db: GraphDatabaseService, execMode: ExecutionMode,
                    descriptionProvider: (InternalPlanDescription) => (Supplier[InternalPlanDescription], Option[QueryExecutionTracer]),
                    params: immutable.Map[String, Any], closer: TaskCloser): InternalExecutionResult = {
            val (supplier, tracer) = descriptionProvider(description)
            GeneratedCodeLoader.newInstance(clazz, closer, statement, db, execMode, supplier,
                              tracer.getOrElse(QueryExecutionTracer.NONE), asJavaHashMap(params))
          }
        }

        val columns = res.nodes ++ res.relationships ++ res.other
        CompiledPlan(updating = false, None, fp, plannerName, description, columns, builder)

      case _ => throw new CantCompileQueryException("Can only compile plans with ProduceResult on top")
    }
  }

  private def asJavaHashMap(params: Map[String, Any]) = {
    val jMap = new util.HashMap[String, Object]()
    params.foreach {
      case (key, value) => jMap.put(key, javaValue(value))
    }
    jMap
  }

  private def javaValue(value: Any): Object = value match {
    case iter: Seq[_] => iter.map(javaValue).asJava
    case iter: Map[_, _] => Eagerly.immutableMapValues(iter, javaValue).asJava
    case x: Any => x.asInstanceOf[AnyRef]
  }

}
