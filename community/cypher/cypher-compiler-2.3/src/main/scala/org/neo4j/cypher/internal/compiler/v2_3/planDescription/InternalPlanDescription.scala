/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.planDescription

import org.neo4j.cypher.internal.compiler.v2_3.commands
import org.neo4j.cypher.internal.compiler.v2_3.pipes.{SeekArgs => PipeEntityByIdRhs}
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription.Arguments._
import org.neo4j.cypher.internal.frontend.v2_3.{SemanticDirection, ast}

/**
 * Abstract description of an execution plan
 */
sealed trait InternalPlanDescription {
  self =>

  def arguments: Seq[Argument]
  def id: Id
  def name: String
  def children: Children
  def identifiers: Set[String]

  def cd(name: String): InternalPlanDescription = children.find(name).head
  def map(f: InternalPlanDescription => InternalPlanDescription): InternalPlanDescription
  def find(name: String): Seq[InternalPlanDescription]
  def addArgument(arg: Argument): InternalPlanDescription

  def flatten: Seq[InternalPlanDescription] = {
    def flattenAcc(acc: Seq[InternalPlanDescription], plan: InternalPlanDescription): Seq[InternalPlanDescription] = {
      plan.children.toSeq.foldLeft(acc :+ plan) {
        case (acc1, plan1) => flattenAcc(acc1, plan1)
      }
    }
    flattenAcc(Seq.empty, this)
  }

  def andThen(id: Id, name: String, identifiers: Set[String], arguments: Argument*) =
    PlanDescriptionImpl(id, name, SingleChild(this), arguments, identifiers)

  def orderedIdentifiers: Seq[String] = identifiers.toSeq.sorted

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
    case class LegacyExpression(value: commands.expressions.Expression) extends Argument
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
                                direction: SemanticDirection, varLength: Boolean = false) extends Argument
    case class SourceCode(className: String, sourceCode: String) extends Argument {
      override def name = className
    }
  }
}

sealed trait Children {
  def isEmpty = toSeq.isEmpty
  def tail = toSeq.tail
  def head = toSeq.head
  def toSeq: Seq[InternalPlanDescription]
  def find(name: String): Seq[InternalPlanDescription] = toSeq.flatMap(_.find(name))
  def map(f: InternalPlanDescription => InternalPlanDescription): Children
  def foreach[U](f: InternalPlanDescription => U) {
    toSeq.foreach(f)
  }
}

case object NoChildren extends Children {
  def toSeq = Seq.empty
  def map(f: InternalPlanDescription => InternalPlanDescription) = NoChildren
}

final case class SingleChild(child: InternalPlanDescription) extends Children {
  val toSeq = Seq(child)
  def map(f: InternalPlanDescription => InternalPlanDescription) = SingleChild(child = child.map(f))
}

final case class TwoChildren(lhs: InternalPlanDescription, rhs: InternalPlanDescription) extends Children {
  val toSeq = Seq(lhs, rhs)
  def map(f: InternalPlanDescription => InternalPlanDescription) = TwoChildren(lhs = lhs.map(f), rhs = rhs.map(f))
}

final case class PlanDescriptionImpl(id: Id,
                                     name: String,
                                     children: Children,
                                     arguments: Seq[Argument],
                                     identifiers: Set[String]) extends InternalPlanDescription {
  def find(name: String): Seq[InternalPlanDescription] =
    children.find(name) ++ (if (this.name == name)
      Some(this)
    else {
      None
    })

  def addArgument(argument: Argument): InternalPlanDescription = copy(arguments = arguments :+ argument)

  def map(f: InternalPlanDescription => InternalPlanDescription): InternalPlanDescription = f(copy(children = children.map(f)))

  def toSeq: Seq[InternalPlanDescription] = this +: children.toSeq

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

final case class SingleRowPlanDescription(id: Id, arguments: Seq[Argument] = Seq.empty, identifiers: Set[String]) extends InternalPlanDescription {
  override def andThen(id: Id, name: String, identifiers: Set[String], newArguments: Argument*) =
    new PlanDescriptionImpl(id, name, NoChildren, newArguments, identifiers)

  def children = NoChildren

  def find(searchedName: String) = if (searchedName == name) Seq(this) else Seq.empty

  def name = "Argument"

  def render(builder: StringBuilder) {}

  def render(builder: StringBuilder, separator: String, levelSuffix: String) {}

  def addArgument(arg: Argument): InternalPlanDescription = copy(arguments = arguments :+ arg)

  def map(f: (InternalPlanDescription) => InternalPlanDescription): InternalPlanDescription = f(this)

  def toSeq: Seq[InternalPlanDescription] = Seq(this)
}
