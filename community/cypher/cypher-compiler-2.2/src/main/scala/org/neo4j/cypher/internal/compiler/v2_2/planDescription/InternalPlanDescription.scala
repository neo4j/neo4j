/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.planDescription

import org.neo4j.cypher.internal.compiler.v2_2.commands
import org.neo4j.cypher.internal.compiler.v2_2.pipes.{Pipe, RonjaPipe, EntityByIdRhs => PipeEntityByIdRhs}
import org.neo4j.cypher.internal.compiler.v2_2.planDescription.InternalPlanDescription.Arguments._

/**
 * Abstract description of an execution plan
 */
sealed trait InternalPlanDescription {
  self =>

  def arguments: Seq[Argument]
  def cd(name: String): InternalPlanDescription = children.find(name).head
  def pipe: Pipe
  def name: String
  def children: Children
  def map(f: InternalPlanDescription => InternalPlanDescription): InternalPlanDescription
  def find(name: String): Seq[InternalPlanDescription]
  def addArgument(arg: Argument): InternalPlanDescription
  def andThen(pipe: Pipe, name: String, arguments: Argument*) = PlanDescriptionImpl(pipe, name, SingleChild(this), arguments)
  def toSeq: Seq[InternalPlanDescription]

  def totalDbHits: Option[Long] = {
    val allMaybeDbHits: Seq[Option[Long]] = toSeq.map {
      case plan: InternalPlanDescription => plan.arguments.collectFirst { case DbHits(x) => x}
    }

    allMaybeDbHits.reduce[Option[Long]] {
      case (a: Option[Long], b: Option[Long]) => for (aVal <- a; bVal <- b) yield aVal + bVal
    }
  }
}

sealed abstract class Argument extends Product {
  def name = productPrefix
}

object InternalPlanDescription {
  object Arguments {
    case class Rows(value: Long) extends Argument
    case class DbHits(value: Long) extends Argument
    case class IntroducedIdentifier(value: String) extends Argument
    case class ColumnsLeft(value: Seq[String]) extends Argument
    case class LegacyExpression(value: commands.expressions.Expression) extends Argument
    case class UpdateActionName(value: String) extends Argument
    case class LegacyIndex(value: String) extends Argument
    case class Index(label: String, property: String) extends Argument
    case class LabelName(label: String) extends Argument
    case class KeyNames(keys: Seq[String]) extends Argument
    case class KeyExpressions(expressions: Seq[commands.expressions.Expression]) extends Argument
    case class EntityByIdRhs(value: PipeEntityByIdRhs) extends Argument
    case class EstimatedRows(value: Long) extends Argument
    case class Version(value: String) extends Argument {
      override def name = "version"
    }
  }
}

sealed trait Children {
  def isEmpty = children.isEmpty
  def tail = children.tail
  def head = children.head
  protected def children: Seq[InternalPlanDescription]
  def find(name: String): Seq[InternalPlanDescription] = children.flatMap(_.find(name))
  def map(f: InternalPlanDescription => InternalPlanDescription): Children
  def foreach(f:InternalPlanDescription=>Unit) {
    children.foreach(f)
  }
  def toSeq:Seq[InternalPlanDescription]
}

case object NoChildren extends Children {
  protected def children = Seq.empty
  def map(f: InternalPlanDescription => InternalPlanDescription) = NoChildren

  def toSeq: Seq[InternalPlanDescription] = Seq.empty
}

final case class SingleChild(child: InternalPlanDescription) extends Children {
  protected def children = Seq(child)
  def map(f: InternalPlanDescription => InternalPlanDescription) = SingleChild(child.map(f))

  def toSeq: Seq[InternalPlanDescription] = child.toSeq
}

final case class TwoChildren(lhs: InternalPlanDescription, rhs: InternalPlanDescription) extends Children {
  protected def children = Seq(lhs, rhs)

  def toSeq: Seq[InternalPlanDescription] = lhs.toSeq ++ rhs.toSeq

  def map(f: InternalPlanDescription => InternalPlanDescription) = TwoChildren(lhs = lhs.map(f), rhs = rhs.map(f))
}

final case class PlanDescriptionImpl(pipe: Pipe,
                                     name: String,
                                     children: Children,
                                     _arguments: Seq[Argument]) extends InternalPlanDescription {

  self =>

  def arguments: Seq[Argument] = _arguments ++ (pipe match {
    case r: RonjaPipe => r.estimatedCardinality.map(EstimatedRows.apply)
    case _            => None
  })

  def find(name: String): Seq[InternalPlanDescription] =
    children.find(name) ++ (if (this.name == name)
      Some(this)
    else {
      None
    })



  def addArgument(argument: Argument): InternalPlanDescription = copy(_arguments = _arguments :+ argument)

  def map(f: InternalPlanDescription => InternalPlanDescription): InternalPlanDescription = f(copy(children = children.map(f)))

  def toSeq: Seq[InternalPlanDescription] = this +: children.toSeq

  override def toString = {
    val treeString = renderAsTree(this)
    val details = renderDetails(this)
    val summary = renderSummary(this)
    "%s%n%n%s%n%s".format(treeString, details, summary)
  }

  def render( builder: StringBuilder, separator: String, levelSuffix: String ) { ??? }

  def render( builder: StringBuilder ) { ??? }
}

final case class ArgumentPlanDescription(pipe: Pipe, arguments: Seq[Argument] = Seq.empty) extends InternalPlanDescription {
  override def andThen(pipe: Pipe, name: String, arguments: Argument*) = new PlanDescriptionImpl(pipe, name, NoChildren, arguments)

  def children = NoChildren

  def find(searchedName: String) = if (searchedName == name) Seq(this) else Seq.empty

  def name = "Argument"

  def render(builder: StringBuilder) {}

  def render(builder: StringBuilder, separator: String, levelSuffix: String) {}

  def addArgument(arg: Argument): InternalPlanDescription = copy(arguments = arguments :+ arg)

  def map(f: (InternalPlanDescription) => InternalPlanDescription): InternalPlanDescription = f(this)

  def toSeq: Seq[InternalPlanDescription] = Seq(this)
}
