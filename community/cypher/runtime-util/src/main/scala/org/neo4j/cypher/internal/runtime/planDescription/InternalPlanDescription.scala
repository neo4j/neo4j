/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.planDescription

import java.util

import org.neo4j.cypher.exceptionHandler
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription.Arguments._
import org.opencypher.v9_0.util.InternalException
import org.opencypher.v9_0.util.attribution.Id
import org.opencypher.v9_0.util.symbols.CypherType
import org.opencypher.v9_0.expressions.SemanticDirection
import org.neo4j.cypher.internal.v3_5.logical.plans.{QualifiedName, SeekableArgs}
import org.neo4j.graphdb.ExecutionPlanDescription
import org.neo4j.graphdb.ExecutionPlanDescription.ProfilerStatistics
import org.opencypher.v9_0.{expressions => ast}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Abstract description of an execution plan
  */
sealed trait InternalPlanDescription extends org.neo4j.graphdb.ExecutionPlanDescription {
  self =>

  def arguments: Seq[Argument]

  def id: Id

  def name: String

  def children: Children

  def variables: Set[String]

  def cd(name: String): InternalPlanDescription = children.find(name).head

  def map(f: InternalPlanDescription => InternalPlanDescription): InternalPlanDescription

  def find(name: String): Seq[InternalPlanDescription]

  def addArgument(arg: Argument): InternalPlanDescription

  def flatten: Seq[InternalPlanDescription] = {
      val flatten = new ArrayBuffer[InternalPlanDescription]
      val stack = new mutable.Stack[InternalPlanDescription]()
      stack.push(self)
      while (stack.nonEmpty) {
        val plan = stack.pop()
        flatten.append(plan)
        plan.children match {
          case NoChildren =>
          case SingleChild(child) =>
            stack.push(child)
          case TwoChildren(l, r) =>
            stack.push(r)
            stack.push(l)
        }
      }
      flatten
  }

  def orderedVariables: Seq[String] = variables.toIndexedSeq.sorted

  def totalDbHits: Option[Long] = {
    val allMaybeDbHits: Seq[Option[Long]] = flatten.map {
      plan: InternalPlanDescription => plan.arguments.collectFirst { case DbHits(x) => x }
    }

    allMaybeDbHits.reduce[Option[Long]] {
      case (a: Option[Long], b: Option[Long]) => for (aVal <- a; bVal <- b) yield aVal + bVal
    }
  }

  //Implement public Java API here=
  override def getName: String = name

  import scala.collection.JavaConverters._

  override def getChildren: util.List[ExecutionPlanDescription] = {
    val childPlans: Seq[org.neo4j.graphdb.ExecutionPlanDescription] = exceptionHandler.runSafely {
      children.toIndexedSeq
    }

    childPlans.asJava
  }

  override def getArguments: util.Map[String, AnyRef] =
    arguments.map { arg => arg.name -> PlanDescriptionArgumentSerializer.serialize(arg) }.toMap.asJava

  override def getIdentifiers: util.Set[String] = orderedVariables.toSet.asJava

  override def hasProfilerStatistics: Boolean = arguments.exists(_.isInstanceOf[DbHits])

  override def getProfilerStatistics: ExecutionPlanDescription.ProfilerStatistics = new ProfilerStatistics {
    def getDbHits: Long = extract { case DbHits(count) => count }.getOrElse(throw new InternalException("Don't have profiler stats"))

    def getRows: Long = extract { case Rows(count) => count }.getOrElse(throw new InternalException("Don't have profiler stats"))

    def getPageCacheHits: Long = extract { case PageCacheHits(count) => count }.getOrElse(0)

    def getPageCacheMisses: Long = extract { case PageCacheMisses(count) => count }.getOrElse(0)

    private def extract(f: PartialFunction[Argument, Long]): Option[Long] = arguments.collectFirst(f)
  }

}

sealed abstract class Argument extends Product {

  def name = productPrefix
}

object InternalPlanDescription {

  object Arguments {

    case class Time(value: Long) extends Argument

    case class Rows(value: Long) extends Argument

    case class DbHits(value: Long) extends Argument

    case class PageCacheHits(value: Long) extends Argument

    case class PageCacheMisses(value: Long) extends Argument

    case class PageCacheHitRatio(value: Double) extends Argument

    case class ColumnsLeft(value: Seq[String]) extends Argument

    case class Expression(value: ast.Expression) extends Argument

    case class Expressions(expressions: Map[String, ast.Expression]) extends Argument

    case class UpdateActionName(value: String) extends Argument

    case class MergePattern(startPoint: String) extends Argument

    case class ExplicitIndex(value: String) extends Argument

    case class Index(label: String, propertyKeys: Seq[String]) extends Argument

    case class PrefixIndex(label: String, propertyKey: String, prefix: ast.Expression) extends Argument

    case class InequalityIndex(label: String, propertyKey: String, bounds: Seq[String]) extends Argument

    case class PointDistanceIndex(label: String, propertyKey: String, point: String, distance: String, inclusive: Boolean) extends Argument

    case class LabelName(label: String) extends Argument

    case class KeyNames(keys: Seq[String]) extends Argument

    case class KeyExpressions(expressions: Seq[Expression]) extends Argument

    case class EntityByIdRhs(value: SeekableArgs) extends Argument

    case class EstimatedRows(value: Double) extends Argument

    case class Signature(procedureName: QualifiedName,
                         args: Seq[ast.Expression],
                         results: Seq[(String, CypherType)]) extends Argument

    // This is the version of cypher and will equal the planner version
    // that is being used.
    case class Version(value: String) extends Argument {

      override def name = "version"
    }

    case class RuntimeVersion(value: String) extends Argument {

      override def name = "runtime-version"
    }

    case class Planner(value: String) extends Argument {

      override def name = "planner"
    }

    case class PlannerImpl(value: String) extends Argument {

      override def name = "planner-impl"
    }

    case class PlannerVersion(value: String) extends Argument {

      override def name = "planner-version"
    }

    case class Runtime(value: String) extends Argument {

      override def name = "runtime"
    }

    case class RuntimeImpl(value: String) extends Argument {

      override def name = "runtime-impl"
    }

    case class ExpandExpression(from: String, relName: String, relTypes: Seq[String], to: String,
                                direction: SemanticDirection, minLength: Int, maxLength: Option[Int]) extends Argument

    case class CountNodesExpression(ident: String, labels: List[Option[String]]) extends Argument

    case class CountRelationshipsExpression(ident: String, startLabel: Option[String],
                                            typeNames: Seq[String], endLabel: Option[String]) extends Argument

    case class SourceCode(className: String, sourceCode: String) extends Argument {

      override def name = "source:" + className
    }

    case class ByteCode(className: String, disassembly: String) extends Argument {

      override def name = "bytecode:" + className
    }

  }

}

sealed trait Children {

  def isEmpty: Boolean = toIndexedSeq.isEmpty

  def tail: Seq[InternalPlanDescription] = toIndexedSeq.tail

  def head: InternalPlanDescription = toIndexedSeq.head

  def toIndexedSeq: Seq[InternalPlanDescription]

  def find(name: String): Seq[InternalPlanDescription] = toIndexedSeq.flatMap(_.find(name))

  def map(f: InternalPlanDescription => InternalPlanDescription): Children

  def foreach[U](f: InternalPlanDescription => U) {
    toIndexedSeq.foreach(f)
  }
}

case object NoChildren extends Children {

  def toIndexedSeq = Seq.empty

  def map(f: InternalPlanDescription => InternalPlanDescription) = NoChildren
}

final case class SingleChild(child: InternalPlanDescription) extends Children {

  val toIndexedSeq = Seq(child)

  def map(f: InternalPlanDescription => InternalPlanDescription) = SingleChild(child = child.map(f))
}

final case class TwoChildren(lhs: InternalPlanDescription, rhs: InternalPlanDescription) extends Children {

  val toIndexedSeq = Seq(lhs, rhs)

  def map(f: InternalPlanDescription => InternalPlanDescription) = TwoChildren(lhs = lhs.map(f), rhs = rhs.map(f))
}

final case class PlanDescriptionImpl(id: Id,
                                     name: String,
                                     children: Children,
                                     arguments: Seq[Argument],
                                     variables: Set[String]) extends InternalPlanDescription {

  def find(name: String): Seq[InternalPlanDescription] =
    children.find(name) ++ (if (this.name == name)
      Some(this)
    else {
      None
    })

  def addArgument(argument: Argument): InternalPlanDescription = copy(arguments = arguments :+ argument)

  def map(f: InternalPlanDescription => InternalPlanDescription): InternalPlanDescription = f(
    copy(children = children.map(f)))

  def toIndexedSeq: Seq[InternalPlanDescription] = this +: children.toIndexedSeq

  val NL = System.lineSeparator()

  override def toString = {
    val version = arguments.collectFirst {
      case Version(v) => s"Compiler $v$NL"
    }
    val planner = arguments.collectFirst {
      case Planner(n) => s"Planner ${n.toUpperCase}$NL"
    }

    val runtime = arguments.collectFirst {
      case Runtime(n) => s"Runtime ${n.toUpperCase}$NL"
    }

    val runtimeVersion = arguments.collectFirst {
      case RuntimeVersion(n) => s"Runtime version ${n.toUpperCase}$NL"
    }
    val prefix = version ++ planner ++ runtime ++ runtimeVersion
    s"${prefix.mkString("", NL, NL)}${renderAsTreeTable(this)}$NL${renderSummary(this)}$renderSources"
  }

  private def renderSources = {
    arguments.flatMap {
      case SourceCode(className, sourceCode) => Some(s"=== Java Source: $className ===$NL$sourceCode")
      case ByteCode(className, byteCode) => Some(s"=== Bytecode: $className ===$NL$byteCode")
      case _ => None
    }.mkString(NL, NL, "")
  }
}

object CompactedPlanDescription {

  def create(similar: Seq[InternalPlanDescription]): InternalPlanDescription =
    if (similar.size == 1) similar.head else CompactedPlanDescription(similar)
}

final case class CompactedPlanDescription(similar: Seq[InternalPlanDescription]) extends InternalPlanDescription {

  override def name: String = s"${similar.head.name}(${similar.size})"

  override lazy val variables: Set[String] = similar.foldLeft(Set.empty[String]) { (acc, plan) =>
    acc ++ plan.variables
  }

  override def children: Children = similar.last.children

  override val arguments: Seq[Argument] = {
    var dbHits: Option[Long] = None
    var pageCacheHits: Option[Long] = None
    var pageCacheMisses: Option[Long] = None
    var pageCacheHitRatio: Option[Double] = None
    var time: Option[Long] = None
    var rows: Option[Long] = None

    similar.foldLeft(Set.empty[Argument]) {
      (acc, plan) =>
        val args = plan.arguments.filter {
          case DbHits(v) => dbHits = Some(dbHits.map(_ + v).getOrElse(v)); false
          case PageCacheHits(v) => pageCacheHits = Some(pageCacheHits.map(_ + v).getOrElse(v)); false
          case PageCacheMisses(v) => pageCacheMisses = Some(pageCacheMisses.map(_ + v).getOrElse(v)); false
          case PageCacheHitRatio(v) => pageCacheHitRatio = Some(pageCacheHitRatio.map(_ + v).getOrElse(v)); false
          case Time(v) => time = Some(time.map(_ + v).getOrElse(v)); false
          case Rows(v) => rows = Some(rows.map(o => Math.max(o, v)).getOrElse(v)); false
          case _ => true
        }
        acc ++ args
    }.toIndexedSeq ++ dbHits.map(DbHits.apply) ++ pageCacheHits.map(PageCacheHits.apply) ++
      pageCacheMisses.map(PageCacheMisses.apply) ++ pageCacheHitRatio.map(PageCacheHitRatio.apply) ++
      time.map(Time.apply) ++ rows.map(Rows.apply)
  }

  override def find(name: String): Seq[InternalPlanDescription] = similar.last.find(name)

  override def id: Id = similar.last.id

  override def addArgument(argument: Argument): InternalPlanDescription = ???

  override def map(f: InternalPlanDescription => InternalPlanDescription): InternalPlanDescription = f(copy
                                                                                                       (similar = similar
                                                                                                         .map(f)))

}

final case class ArgumentPlanDescription(id: Id,
                                         arguments: Seq[Argument] = Seq.empty,
                                         variables: Set[String])
  extends InternalPlanDescription {

  def children = NoChildren

  def find(searchedName: String) = if (searchedName == name) Seq(this) else Seq.empty

  def name = "EmptyRow"

  def render(builder: StringBuilder) {}

  def render(builder: StringBuilder, separator: String, levelSuffix: String) {}

  def addArgument(arg: Argument): InternalPlanDescription = copy(arguments = arguments :+ arg)

  def map(f: (InternalPlanDescription) => InternalPlanDescription): InternalPlanDescription = f(this)

  def toIndexedSeq: Seq[InternalPlanDescription] = Seq(this)
}
