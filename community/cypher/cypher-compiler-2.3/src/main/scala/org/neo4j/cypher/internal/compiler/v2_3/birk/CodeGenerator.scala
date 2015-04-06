/**
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

import java.util.concurrent.atomic.AtomicInteger

import org.neo4j.cypher.internal.compiler.v2_3.CostPlannerName
import org.neo4j.cypher.internal.compiler.v2_3.birk.CodeGenerator.JavaTypes.{INT, LONG}
import org.neo4j.cypher.internal.compiler.v2_3.birk.il._
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{PlanFingerprint, CompiledPlan}
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compiler.v2_3.planner.CantCompileQueryException
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.{LogicalPlanIdentificationBuilder,
LogicalPlan2PlanDescription}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_3.spi.{PlanContext, InstrumentedGraphStatistics}
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.helpers.Clock
import org.neo4j.kernel.api.Statement

import scala.collection.immutable.Stack

object CodeGenerator {
  private val packageName = "org.neo4j.cypher.internal.compiler.v2_3.birk.generated"
  private val nameCounter = new AtomicInteger(0)

  def nextClassName(): String = {
    val x = nameCounter.getAndIncrement
    s"GeneratedExecutionPlan$x"
  }

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

  object JavaTypes {
    def LONG = "long"
    def INT = "int"
  }
}

class Namer(prefix: String) {
  var varCounter = 0

  def next(): String = {
    varCounter += 1
    prefix + varCounter
  }

  def nextWithType(typ: String): JavaSymbol = JavaSymbol(next(), typ)
}

case class JavaSymbol(name: String, javaType: String)

class CodeGenerator {

  import CodeGenerator.{n, nextClassName, packageName}

  def generate(plan: LogicalPlan, planContext: PlanContext, clock: Clock) = {
    plan match {
      case _: ProduceResult =>
        val className = nextClassName()
        val planStatements: Seq[Instruction] = createResultAst(plan)
        val source = generateCodeFromAst(className, planStatements)
        val clazz = Javac.compile(s"$packageName.$className",source )

        val fp = planContext.statistics match {
          case igs: InstrumentedGraphStatistics =>
            Some(PlanFingerprint(clock.currentTimeMillis(), planContext.txIdProvider(), igs.snapshot.freeze))
          case _ =>
            None
        }

        val idMap = LogicalPlanIdentificationBuilder(plan)
        val description: InternalPlanDescription = LogicalPlan2PlanDescription(plan, idMap)

        val builder = (st: Statement, db: GraphDatabaseService) => Javac.newInstance(clazz, st, db, description)

        CompiledPlan(updating = false, None, fp, CostPlannerName, builder)

      case _ => throw new CantCompileQueryException("Can only compile plans with ProduceResult on top")
    }
  }

  private def generateCodeFromAst(className: String, statements: Seq[Instruction]) = {
    val importLines: Set[String] =
      statements.
        map(_.importedClasses()).
        reduceOption(_ ++ _).
        getOrElse(Set.empty)

    val imports = if(importLines.nonEmpty)
      importLines.toSeq.sorted.mkString("import ", s";${n}import ", ";")
    else
      ""
    val fields = statements.map(_.fields().trim).reduce(_ + n + _)
    val init = statements.map(_.generateInit().trim).reduce(_ + n + _)
    val methodBody = statements.map(_.generateCode().trim).reduce(_ + n + _)
    val privateMethods = statements.flatMap(_.methods).distinct.sortBy(_.name)
    val privateMethodText = privateMethods.map(_.generateCode.trim).reduceOption(_ + n + _).getOrElse("")

    //TODO move imports to set and merge with the other imports, or use full paths
    s"""package $packageName;
       |
       |import org.neo4j.helpers.collection.Visitor;
       |import org.neo4j.graphdb.GraphDatabaseService;
       |import org.neo4j.kernel.api.Statement;
       |import org.neo4j.kernel.api.exceptions.KernelException;
       |import org.neo4j.kernel.api.ReadOperations;
       |import org.neo4j.cypher.internal.compiler.v2_3.birk.ResultRowImpl;
       |import org.neo4j.cypher.internal.compiler.v2_3.executionplan.CompiledExecutionResult;
       |import org.neo4j.graphdb.Result.ResultRow;
       |import org.neo4j.graphdb.Result.ResultVisitor;
       |import org.neo4j.graphdb.Result;
       |import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription;
       |$imports
       |
       |public class $className extends CompiledExecutionResult
       |{
       |private final ReadOperations ro;
       |private final GraphDatabaseService db;
       |private final InternalPlanDescription description;
       |
       |public $className( Statement statement, GraphDatabaseService db, InternalPlanDescription description )
       |{
       |  this.ro = statement.readOperations( );
       |  this.db = db;
       |  this.description = description;
       |}
       |
       |public InternalPlanDescription executionPlanDescription()
       |{
       |  return this.description;
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
       |}
       |catch (Exception e)
       |{
       |//TODO proper error handling
       |throw new RuntimeException( e );
       |}
       |}
       |$privateMethodText
       |}""".stripMargin
  }

  private def createResultAst(plan: LogicalPlan): Seq[Instruction] = {
    var variables: Map[String, JavaSymbol] = Map.empty
    var probeTables: Map[NodeHashJoin, CodeThunk] = Map.empty
    val variableName = new Namer("v")
    val methodName = new Namer("m")

    def produce(plan: LogicalPlan, stack: Stack[LogicalPlan]): (Option[JavaSymbol], Seq[Instruction]) = {
      plan match {
        case AllNodesScan(IdName(name), arguments) =>
          val variable = variableName.nextWithType(LONG)
          variables += (name -> variable)
          val (methodHandle, actions) = consume(stack.top, plan, stack.pop)
          (methodHandle, Seq(WhileLoop(variable, ScanAllNodes(), actions)))

        case NodeByLabelScan(IdName(name), label, _) =>
          val nodeVariable = variableName.nextWithType(LONG)
          val labelToken = variableName.nextWithType(INT)
          variables += (name -> nodeVariable)
          val (methodHandle, actions) = consume(stack.top, plan, stack.pop)
          (methodHandle, Seq(WhileLoop(nodeVariable, ScanForLabel(label.name, labelToken), actions)))

        case _: ProduceResult | _: Expand =>
          produce(plan.lhs.get, stack.push(plan))

        case NodeHashJoin(_, lhs, rhs) =>
          val (Some(symbol), lAst) = produce(lhs, stack.push(plan))
          val lhsMethod = MethodInvocation(symbol.name, symbol.javaType, methodName.next(), lAst)
          val (x, r) = produce(rhs, stack.push(plan))
          (x, lhsMethod +: r)

        case _ => throw new CantCompileQueryException(s"$plan is not yet supported")
      }
    }

    def consume(plan: LogicalPlan, from: LogicalPlan, stack: Stack[LogicalPlan]): (Option[JavaSymbol], Instruction) = {
      plan match {
        case ProduceResult(nodes, rels, _) =>
          (None, ProduceResults(nodes.map(c => c -> variables(c).name).toMap,
            rels.map(c => c -> variables(c).name).toMap))

        case join@NodeHashJoin(nodes, lhs, rhs) if from eq lhs =>
          val nodeId = variables(nodes.head.name)
          val probeTableName = variableName.next()
          val symbols = (lhs.availableSymbols.map(_.name) intersect variables.keySet diff nodes.map(_.name)).map(s => s -> variables(s)).toMap

          val probeTable = BuildProbeTable(probeTableName, nodeId.name, symbols, variableName)
          val probeTableSymbol = JavaSymbol(probeTableName, probeTable.producedType)

          probeTables += (join -> probeTable.generateFetchCode)

          (Some(probeTableSymbol), probeTable)

        case join@NodeHashJoin(nodes, lhs, rhs) if from eq rhs =>
          val nodeId = variables(nodes.head.name)
          val thunk = probeTables(join)
          thunk.vars foreach(variables += _)
          val (x, action) = consume(stack.top, plan, stack.pop)

          (x, GetMatchesFromProbeTable(nodeId.name, thunk, action))

        case Expand(_, IdName(fromNode), dir, relTypes, IdName(to), IdName(rel), ExpandAll) =>
          val relVar = variableName.nextWithType(LONG)
          val nodeVar = variableName.nextWithType(LONG)
          variables  += rel -> relVar
          variables  += to -> nodeVar

          val (x, action) = consume(stack.top, plan, stack.pop)
          (x, WhileLoop(relVar, ExpandC(variables(fromNode).name, relVar.name, dir, relTypes.map(t => variableName.next -> t.name).toMap,nodeVar.name, action), Instruction.empty))

        case _ => throw new CantCompileQueryException(s"$plan is not yet supported")
      }
    }

    val (_, result) = produce(plan, Stack.empty)

    result
  }
}
