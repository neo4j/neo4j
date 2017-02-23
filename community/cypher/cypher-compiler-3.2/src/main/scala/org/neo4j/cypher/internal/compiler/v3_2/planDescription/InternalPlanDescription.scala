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
package org.neo4j.cypher.internal.compiler.v3_2.planDescription

import org.neo4j.cypher.internal.compiler.v3_2.commands
import org.neo4j.cypher.internal.compiler.v3_2.pipes.{SeekArgs => PipeEntityByIdRhs}
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.InternalPlanDescription.Arguments._
import org.neo4j.cypher.internal.compiler.v3_2.spi.QualifiedName
import org.neo4j.cypher.internal.frontend.v3_2.symbols.CypherType
import org.neo4j.cypher.internal.frontend.v3_2.{SemanticDirection, ast}

/**
 * Abstract description of an execution plan
 */
sealed trait InternalPlanDescription {
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
    def flattenAcc(acc: Seq[InternalPlanDescription], plan: InternalPlanDescription): Seq[InternalPlanDescription] = {
      plan.children.toIndexedSeq.foldLeft(acc :+ plan) {
        case (acc1, plan1) => flattenAcc(acc1, plan1)
      }
    }
    flattenAcc(Seq.empty, this)
  }

  def orderedVariables: Seq[String] = variables.toIndexedSeq.sorted

  def totalDbHits: Option[Long] = {
    val allMaybeDbHits: Seq[Option[Long]] = flatten.map {
      case plan: InternalPlanDescription => plan.arguments.collectFirst { case DbHits(x) => x}
    }

    allMaybeDbHits.reduce[Option[Long]] {
      case (a: Option[Long], b: Option[Long]) => for (aVal <- a; bVal <- b) yield aVal + bVal
    }
  }
}

class Id

sealed abstract class Argument extends Product {
  def name = productPrefix
}

object InternalPlanDescription {
  object Arguments {
    case class Time(value: Long) extends Argument
    case class Rows(value: Long) extends Argument
    case class DbHits(value: Long) extends Argument
    case class ColumnsLeft(value: Seq[String]) extends Argument
    case class Expression(value: ast.Expression) extends Argument
    case class Expressions(expressions: Map[String, ast.Expression]) extends Argument
    case class LegacyExpression(value: commands.expressions.Expression) extends Argument
    case class LegacyExpressions(expressions: Map[String, commands.expressions.Expression]) extends Argument {
      override def name = "LegacyExpression"
    }
    case class UpdateActionName(value: String) extends Argument
    case class MergePattern(startPoint: String) extends Argument
    case class LegacyIndex(value: String) extends Argument
    case class Index(label: String, propertyKey: String) extends Argument
    case class PrefixIndex(label: String, propertyKey: String, prefix: commands.expressions.Expression) extends Argument
    case class InequalityIndex(label: String, propertyKey: String, bounds: Seq[String]) extends Argument
    case class LabelName(label: String) extends Argument
    case class KeyNames(keys: Seq[String]) extends Argument
    case class KeyExpressions(expressions: Seq[commands.expressions.Expression]) extends Argument
    case class EntityByIdRhs(value: PipeEntityByIdRhs) extends Argument
    case class EstimatedRows(value: Double) extends Argument
    case class Signature(procedureName: QualifiedName,
                         args: Seq[ast.Expression],
                         results: Seq[(String, CypherType)]) extends Argument
    case class Version(value: String) extends Argument {
      override def name = "version"
    }
    case class Planner(value: String) extends Argument{
      override def name = "planner"
    }
    case class PlannerImpl(value: String) extends Argument{
      override def name = "planner-impl"
    }
    case class Runtime(value: String) extends Argument{
      override def name = "runtime"
    }
    case class RuntimeImpl(value: String) extends Argument{
      override def name = "runtime-impl"
    }
    case class ExpandExpression(from: String, relName: String, relTypes:Seq[String], to: String,
                                direction: SemanticDirection, minLength: Int, maxLength: Option[Int]) extends Argument
    case class CountNodesExpression(ident: String, label: Option[String]) extends Argument
    case class CountRelationshipsExpression(ident: String, startLabel: Option[String],
                                            typeNames: Seq[String], endLabel: Option[String]) extends Argument
    case class SourceCode(className: String, sourceCode: String) extends Argument {
      override def name = "source:" + className
    }
  }
}

sealed trait Children {
  def isEmpty = toIndexedSeq.isEmpty
  def tail = toIndexedSeq.tail
  def head = toIndexedSeq.head
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

  def map(f: InternalPlanDescription => InternalPlanDescription): InternalPlanDescription = f(copy(children = children.map(f)))

  def toIndexedSeq: Seq[InternalPlanDescription] = this +: children.toIndexedSeq

  val NL = System.lineSeparator()

  override def toString = {
    s"${renderAsTreeTable(this)}$NL${renderSummary(this)}$renderSources"
  }

  def render( builder: StringBuilder, separator: String, levelSuffix: String ) { ??? }

  def render( builder: StringBuilder ) { ??? }

  private def renderSources = {
    arguments.flatMap {
      case SourceCode(className, sourceCode) => Some(s"=== Compiled: $className ===$NL$sourceCode")
      case _ => None
    }.mkString(NL,NL,"")
  }
}

object CompactedPlanDescription {
  def create(similar: Seq[InternalPlanDescription]): InternalPlanDescription =
    if (similar.size == 1) similar.head else CompactedPlanDescription(similar)
}

final case class CompactedPlanDescription(similar: Seq[InternalPlanDescription]) extends InternalPlanDescription {

  override def name: String = s"${similar.head.name}(${similar.size})"

  override def variables: Set[String] = similar.foldLeft(Set.empty[String]){ (acc, plan) =>
    acc ++ plan.variables
  }

  override def children: Children = similar.last.children

  override val arguments: Seq[Argument] = {
    var dbHits: Option[Long] = None
    var time: Option[Long] = None
    var rows: Option[Long] = None

    similar.foldLeft(Set.empty[Argument]) {
      (acc, plan) =>
        val args = plan.arguments.filter {
          case DbHits(v) => dbHits = Some(dbHits.map(_ + v).getOrElse(v)); false
          case Time(v) => time = Some(time.map(_ + v).getOrElse(v)); false
          case Rows(v) => rows = Some(rows.map(o => Math.max(o, v)).getOrElse(v)); false
          case _ => true
        }
        acc ++ args
    }.toIndexedSeq ++ dbHits.map(DbHits.apply) ++ time.map(Time.apply) ++ rows.map(Rows.apply)
  }

  override def find(name: String): Seq[InternalPlanDescription] = similar.last.find(name)

  override def id: Id = similar.last.id

  override def addArgument(argument: Argument): InternalPlanDescription = ???

  override def map(f: InternalPlanDescription => InternalPlanDescription): InternalPlanDescription = f(copy
  (similar = similar.map(f)))

}

final case class SingleRowPlanDescription(id: Id, arguments: Seq[Argument] = Seq.empty, variables: Set[String]) extends InternalPlanDescription {
  def children = NoChildren

  def find(searchedName: String) = if (searchedName == name) Seq(this) else Seq.empty

  def name = "EmptyRow"

  def render(builder: StringBuilder) {}

  def render(builder: StringBuilder, separator: String, levelSuffix: String) {}

  def addArgument(arg: Argument): InternalPlanDescription = copy(arguments = arguments :+ arg)

  def map(f: (InternalPlanDescription) => InternalPlanDescription): InternalPlanDescription = f(this)

  def toIndexedSeq: Seq[InternalPlanDescription] = Seq(this)
}
