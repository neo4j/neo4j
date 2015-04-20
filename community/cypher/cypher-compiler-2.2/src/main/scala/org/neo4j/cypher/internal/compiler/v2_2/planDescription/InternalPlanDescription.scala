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
package org.neo4j.cypher.internal.compiler.v2_2.planDescription

import org.neo4j.cypher.internal.compiler.v2_2.commands
import org.neo4j.cypher.internal.compiler.v2_2.pipes.{Pipe, RonjaPipe, EntityByIdRhs => PipeEntityByIdRhs}
import org.neo4j.cypher.internal.compiler.v2_2.planDescription.InternalPlanDescription.Arguments._
import org.neo4j.graphdb.Direction

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

  def flatten: Seq[InternalPlanDescription] = {
    def flattenAcc(acc: Seq[InternalPlanDescription], plan: InternalPlanDescription): Seq[InternalPlanDescription] = {
      plan.children.toSeq.foldLeft(acc :+ plan) {
        case (acc1, plan1) => flattenAcc(acc1, plan1)
      }
    }
    flattenAcc(Seq.empty, this)
  }

  def andThen(pipe: Pipe, name: String, identifiers: Set[String], arguments: Argument*) =
    PlanDescriptionImpl(pipe, name, SingleChild(this), arguments, identifiers)

  def identifiers: Set[String]
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

sealed abstract class Argument extends Product {
  def name = productPrefix
}

object InternalPlanDescription {
  object Arguments {
    case class Rows(value: Long) extends Argument
    case class DbHits(value: Long) extends Argument
    case class ColumnsLeft(value: Seq[String]) extends Argument
    case class LegacyExpression(value: commands.expressions.Expression) extends Argument
    case class UpdateActionName(value: String) extends Argument
    case class LegacyIndex(value: String) extends Argument
    case class Index(label: String, property: String) extends Argument
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
    case class ExpandExpression(from: String, relName: String, relTypes:Seq[String], to: String, direction: Direction, varLength: Boolean = false) extends Argument
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

final case class PlanDescriptionImpl(pipe: Pipe,
                                     name: String,
                                     children: Children,
                                     _arguments: Seq[Argument],
                                     identifiers: Set[String]) extends InternalPlanDescription {

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

final case class SingleRowPlanDescription(pipe: Pipe, arguments: Seq[Argument] = Seq.empty, identifiers: Set[String]) extends InternalPlanDescription {
  override def andThen(pipe: Pipe, name: String, identifiers: Set[String], newArguments: Argument*) =
    new PlanDescriptionImpl(pipe = pipe, name = name, children = NoChildren, _arguments = newArguments, identifiers = identifiers)

  def children = NoChildren

  def find(searchedName: String) = if (searchedName == name) Seq(this) else Seq.empty

  def name = "Argument"

  def render(builder: StringBuilder) {}

  def render(builder: StringBuilder, separator: String, levelSuffix: String) {}

  def addArgument(arg: Argument): InternalPlanDescription = copy(arguments = arguments :+ arg)

  def map(f: (InternalPlanDescription) => InternalPlanDescription): InternalPlanDescription = f(this)

  def toSeq: Seq[InternalPlanDescription] = Seq(this)
}
