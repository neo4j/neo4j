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

import org.neo4j.cypher.internal.compiler.v2_2.pipes.{EntityByIdRhs => PipeEntityByIdRhs, RonjaPipe, Pipe}
import org.neo4j.cypher
import org.neo4j.cypher.internal.compiler.v2_2.planDescription.PlanDescription.Arguments._
import org.neo4j.cypher.internal.compiler.v2_2.commands
import org.neo4j.cypher.javacompat.{PlanDescription => JPlanDescription, ProfilerStatistics}
import java.util
import org.neo4j.cypher.InternalException
import collection.JavaConverters._

/**
 * Abstract description of an execution plan
 */
sealed trait PlanDescription extends cypher.PlanDescription {
  self =>

  def arguments: Seq[Argument]
  def cd(name: String): PlanDescription = children.find(name).head
  def pipe: Pipe
  def children: Children
  def map(f: PlanDescription => PlanDescription): PlanDescription
  def find(name: String): Seq[PlanDescription]
  def addArgument(arg: Argument): PlanDescription
  def andThen(pipe: Pipe, name: String, arguments: Argument*) = PlanDescriptionImpl(pipe, name, SingleChild(this), arguments)
  def toSeq: Seq[PlanDescription]

  lazy val asJava: JPlanDescription = new JPlanDescription {

    def getChildren: util.List[JPlanDescription] = children.toSeq.toList.map(_.asJava).asJava
    def getArguments: util.Map[String, AnyRef] = arguments.map(arg =>
      arg.name -> PlanDescriptionArgumentSerializer.serialize(arg).asInstanceOf[AnyRef]
    ).toMap.asJava

    def hasProfilerStatistics: Boolean = arguments.exists(_.isInstanceOf[DbHits])

    def getName: String = name

    def getProfilerStatistics: ProfilerStatistics = new ProfilerStatistics {
      def getDbHits: Long = arguments.collectFirst { case DbHits(count) => count }.getOrElse(throw new InternalException("Don't have profiler stats"))
      def getRows: Long = arguments.collectFirst { case Rows(count) => count }.getOrElse(throw new InternalException("Don't have profiler stats"))
    }

    override def toString = self.toString
  }
}

sealed abstract class Argument extends Product {
  def name = productPrefix
}

object PlanDescription {
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
  }
}

sealed trait Children {
  def isEmpty = children.isEmpty
  def tail = children.tail
  def head = children.head
  protected def children: Seq[PlanDescription]
  def find(name: String): Seq[PlanDescription] = children.flatMap(_.find(name))
  def map(f: PlanDescription => PlanDescription): Children
  def foreach(f:PlanDescription=>Unit) {
    children.foreach(f)
  }
  def toSeq:Seq[PlanDescription]
}

case object NoChildren extends Children {
  protected def children = Seq.empty
  def map(f: PlanDescription => PlanDescription) = NoChildren

  def toSeq: Seq[PlanDescription] = Seq.empty
}

final case class SingleChild(child: PlanDescription) extends Children {
  protected def children = Seq(child)
  def map(f: PlanDescription => PlanDescription) = SingleChild(child.map(f))

  def toSeq: Seq[PlanDescription] = child.toSeq
}

final case class TwoChildren(lhs: PlanDescription, rhs: PlanDescription) extends Children {
  protected def children = Seq(lhs, rhs)

  def toSeq: Seq[PlanDescription] = lhs.toSeq ++ rhs.toSeq

  def map(f: PlanDescription => PlanDescription) = TwoChildren(lhs = lhs.map(f), rhs = rhs.map(f))
}

final case class PlanDescriptionImpl(pipe: Pipe,
                                     name: String,
                                     children: Children,
                                     _arguments: Seq[Argument]) extends PlanDescription {

  self =>

  def arguments: Seq[Argument] = _arguments ++ (pipe match {
    case r: RonjaPipe => r.estimatedCardinality.map(EstimatedRows.apply)
    case _            => None
  })

  def find(name: String): Seq[PlanDescription] =
    children.find(name) ++ (if (this.name == name)
      Some(this)
    else {
      None
    })



  def addArgument(argument: Argument): PlanDescription = copy(_arguments = _arguments :+ argument)

  def map(f: PlanDescription => PlanDescription): PlanDescription = f(copy(children = children.map(f)))

  def toSeq: Seq[PlanDescription] = this +: children.toSeq

  override def toString = {
    val treeString = renderAsTree(this)
    val details = renderDetails(this)
    val summary = renderSummary(this)
    "%s%n%n%s%n%s".format(treeString, details, summary)
  }

  override def render( builder: StringBuilder, separator: String, levelSuffix: String ): Unit = ???

  override def render( builder: StringBuilder ): Unit = ???
}

final case class ArgumentPlanDescription(pipe: Pipe, arguments: Seq[Argument] = Seq.empty) extends PlanDescription {
  override def andThen(pipe: Pipe, name: String, arguments: Argument*) = new PlanDescriptionImpl(pipe, name, NoChildren, arguments)

  def children = NoChildren

  def find(searchedName: String) = if (searchedName == name) Seq(this) else Seq.empty

  def name = "Argument"

  def render(builder: StringBuilder) {}

  def render(builder: StringBuilder, separator: String, levelSuffix: String) {}

  def addArgument(arg: Argument): PlanDescription = copy(arguments = arguments :+ arg)

  def map(f: (PlanDescription) => PlanDescription): PlanDescription = f(this)

  def toSeq: Seq[PlanDescription] = Seq(this)
}
