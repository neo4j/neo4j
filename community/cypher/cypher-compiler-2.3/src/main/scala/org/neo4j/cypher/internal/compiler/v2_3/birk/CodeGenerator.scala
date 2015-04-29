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
package org.neo4j.cypher.internal.compiler.v2_3.birk

import java.util
import java.util.concurrent.atomic.AtomicInteger

import org.neo4j.cypher.internal.compiler.v2_3.birk.codegen.{CodeGenContext, Namer, setStaticField}
import org.neo4j.cypher.internal.compiler.v2_3.birk.il._
import org.neo4j.cypher.internal.compiler.v2_3.executionplan._
import org.neo4j.cypher.internal.compiler.v2_3.helpers.Eagerly
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.{Id, InternalPlanDescription}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.{LogicalPlan2PlanDescription, LogicalPlanIdentificationBuilder}
import org.neo4j.cypher.internal.compiler.v2_3.planner.{CantCompileQueryException, SemanticTable}
import org.neo4j.cypher.internal.compiler.v2_3.spi.{InstrumentedGraphStatistics, PlanContext}
import org.neo4j.cypher.internal.compiler.v2_3.{ExecutionMode, GreedyPlannerName}
import org.neo4j.function.Supplier
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.helpers.Clock
import org.neo4j.kernel.api.Statement

import scala.collection.{Map, immutable, mutable}

object CodeGenerator {
  def generateClass(instructions: Seq[Instruction]) = {
    val className = Namer.newClassName()
    val source = generateCodeFromInstructions(className, instructions)
    println(indentNicely(source))
    Javac.compile(s"$packageName.$className", source)
  }

  object JavaTypes {
    val LONG = "long"
    val INT = "int"
    val OBJECT = "Object"
    val OBJECT_ARRAY = "Object[]"
    val MAP = "java.util.Map"
    val DOUBLE = "double"
    val STRING = "String"
    val NUMBER = "Number"
  }

  private val packageName = "org.neo4j.cypher.internal.compiler.v2_3.birk.generated"
  private val nameCounter = new AtomicInteger(0)

  def indentNicely(in: String): String = {

    var indent = 0

    in.split(n).flatMap {
      line =>
        val l = line.stripPrefix(" ")
        if (l == "")
          None
        else {
          if (l == "}")
            indent = indent - 1

          val result = "  " * indent + l

          if (l == "{")
            indent = indent + 1

          Some(result)
        }
    }.mkString(n)
  }

  def n = System.lineSeparator()

  private def generateCodeFromInstructions(className: String, instructions: Seq[Instruction]) = {
    val importLines: Set[String] =
      instructions.
        map(_.importedClasses()).
        reduceOption(_ ++ _).
        getOrElse(Set.empty)

    def flatten[T](seq:Seq[Instruction], mapper:(Instruction=>Option[T])) : Seq[T] = {
      seq.flatMap( x => mapper(x) ++ flatten(x.children, mapper))
    }

    val imports = if (importLines.nonEmpty)
      importLines.toSeq.sorted.mkString("import ", s";${n}import ", ";")
    else
      ""
    val fields = instructions.map(_.fields().trim).reduce(_ + n + _)
    val init = instructions.map(_.generateInit().trim).reduce(_ + n + _)
    val methodBody = instructions.map(_.generateCode().trim).reduce(_ + n + _)
    val privateMethods = instructions.flatMap(_.methods).distinct.sortBy(_.name)
    val privateMethodText = privateMethods.map(_.generateCode.trim).reduceOption(_ + n + _).getOrElse("")
    val opIds = flatten(instructions, _.operatorId.map(x => s"public static Id $x;")).toSet.mkString("\n")

    s"""package $packageName;
       |
       |import org.neo4j.helpers.collection.Visitor;
       |import org.neo4j.function.Supplier;
       |import org.neo4j.graphdb.GraphDatabaseService;
       |import org.neo4j.kernel.api.Statement;
       |import org.neo4j.kernel.api.exceptions.KernelException;
       |import org.neo4j.kernel.api.ReadOperations;
       |import org.neo4j.cypher.internal.compiler.v2_3.birk.ResultRowImpl;
       |import org.neo4j.cypher.internal.compiler.v2_3.executionplan.CompiledExecutionResult;
       |import org.neo4j.graphdb.Result.ResultRow;
       |import org.neo4j.graphdb.Result.ResultVisitor;
       |import org.neo4j.graphdb.Result;
       |import org.neo4j.graphdb.Transaction;
       |import org.neo4j.cypher.internal.compiler.v2_3.planDescription.Id;
       |import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription;
       |import org.neo4j.cypher.internal.compiler.v2_3.ExecutionMode;
       |import org.neo4j.cypher.internal.compiler.v2_3.executionplan.CompletionListener;
       |import org.neo4j.cypher.internal.compiler.v2_3.birk.QueryExecutionTracer;
       |import org.neo4j.cypher.internal.compiler.v2_3.birk.QueryExecutionEvent;
       |import java.util.Map;
       |import java.util.List;
       |
       |$imports
       |
       |public class $className extends CompiledExecutionResult
       |{
       |private final ReadOperations ro;
       |private final GraphDatabaseService db;
       |private final Map<String, Object> params;
       |private final QueryExecutionTracer tracer;
       |
       |$opIds
       |
       |public $className( CompletionListener completion, Statement statement, GraphDatabaseService db, ExecutionMode executionMode, Supplier<InternalPlanDescription> description, QueryExecutionTracer tracer, Map<String, Object> params )
       |{
       |  super( completion, statement, executionMode, description );
       |  this.ro = statement.readOperations();
       |  this.db = db;
       |  this.tracer = tracer;
       |  this.params = params;
       |}
       |
       |$fields
       |
       |@Override
       |public <E extends Exception> void accept(final ResultVisitor<E> visitor)
       |{
       |final ResultRowImpl row = new ResultRowImpl(db);
       |$init
       |try
       |{
       |$methodBody
       |success();
       |}
       |catch (Exception e)
       |{
       |//TODO proper error handling
       |e.printStackTrace();
       |throw new RuntimeException( e );
       |}
       |finally
       |{
       |close();
       |}
       |}
       |$privateMethodText
       |}""".stripMargin
  }

  private def createInstructions(plan: LogicalPlan, semanticTable: SemanticTable, idMap: immutable.Map[LogicalPlan, Id]): (Seq[Instruction], mutable.Map[Id, String]) = {
    import org.neo4j.cypher.internal.compiler.v2_3.birk.codegen.LogicalPlanConverter._

    val context = new CodeGenContext(semanticTable, idMap)
    val (_, result) = plan.asCodeGenPlan.produce(context)
    (result, context.operatorIds)
  }
}

case class JavaSymbol(name: String, javaType: String)

class CodeGenerator {

  import CodeGenerator.{createInstructions, generateClass}

  import scala.collection.JavaConverters._

  def generate(plan: LogicalPlan, planContext: PlanContext, clock: Clock, semanticTable: SemanticTable) = {
    plan match {
      case _: ProduceResult =>
        val idMap = LogicalPlanIdentificationBuilder(plan)
        val (instructions, operatorMap) = createInstructions(plan, semanticTable, idMap)
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
                    params: immutable.Map[String, Any], completionListener: CompletionListener): InternalExecutionResult = {
            val (supplier, tracer) = descriptionProvider(description)
            Javac.newInstance(clazz, completionListener, statement, db, execMode, supplier, tracer.getOrElse(QueryExecutionTracer.NONE), asJavaHashMap(params))
          }

        }

        CompiledPlan(updating = false, None, fp, GreedyPlannerName, builder)

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
