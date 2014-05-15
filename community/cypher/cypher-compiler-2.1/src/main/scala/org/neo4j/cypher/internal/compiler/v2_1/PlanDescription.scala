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
package org.neo4j.cypher.internal.compiler.v2_1

import pipes.Pipe
import org.neo4j.cypher
import org.neo4j.cypher.javacompat.{PlanDescription => JPlanDescription, ProfilerStatistics}
import java.util
import org.neo4j.cypher.internal.compiler.v2_1.PlanDescription.Arguments.{Rows, DbHits}
import org.neo4j.cypher.InternalException
import collection.JavaConverters._

/**
 * Abstract description of an execution plan
 */
sealed trait PlanDescription extends cypher.PlanDescription {
  def arguments: Seq[Argument]
  def cd(name: String): PlanDescription = children.find(name).head
  def pipe: Pipe
  def children: Children
  def map(f: PlanDescription => PlanDescription): PlanDescription
  def find(name: String): Seq[PlanDescription]
  def addArgument(arg: Argument): PlanDescription
  def andThen(pipe: Pipe, name: String, arguments: Argument*) = PlanDescriptionImpl(pipe, name, SingleChild(this), arguments)
  def toSeq: Seq[PlanDescription]
}

sealed abstract class Argument extends Product {
  def value: Any
  def name = productPrefix

  // values returned from calling serializableValue must be serializable to JSON by REST server

  def serializableValue: AnyRef
}

sealed abstract class LongArgument extends Argument {
  override def value: Long
  def serializableValue: AnyRef = Long.box(value)
}

sealed abstract class ToStringArgument extends Argument {
 override def serializableValue: String = value.toString
}

sealed abstract class StringSeqArgument extends Argument {
  override def value: Seq[String]
  override def serializableValue: Array[String] = value.toArray
}

sealed abstract class ExpressionSeqArgument extends Argument {
  override def value: Seq[commands.expressions.Expression]
  override def serializableValue: Array[String] = value.map(_.toString).toArray
}

object PlanDescription {
  object Arguments {
    case class Rows(value: Long) extends LongArgument
    case class DbHits(value: Long) extends LongArgument
    case class IntroducedIdentifier(value: String) extends ToStringArgument
    case class ColumnsLeft(value: Seq[String]) extends StringSeqArgument
    case class Expression(value: ast.Expression) extends ToStringArgument
    case class LegacyExpression(value: commands.expressions.Expression) extends ToStringArgument
    case class UpdateActionName(value: String) extends ToStringArgument
    case class LegacyIndex(value: String) extends ToStringArgument
    case class Index(value: String, property: String) extends Argument {
      override def serializableValue = s".$property = $value"
    }
    case class LabelName(value: String) extends ToStringArgument
    case class KeyNames(value: Seq[String]) extends StringSeqArgument
    case class KeyExpressions(value: Seq[commands.expressions.Expression]) extends ExpressionSeqArgument
    case class LimitCount(value: Long) extends LongArgument
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
                                     arguments: Seq[Argument]) extends PlanDescription {

  def find(name: String): Seq[PlanDescription] =
    children.find(name) ++ (if (this.name == name)
      Some(this)
    else {
      None
    })

  val self = this

  lazy val asJava: JPlanDescription = new JPlanDescription {
    def getChildren: util.List[JPlanDescription] = children.toSeq.toList.map(_.asJava).asJava
    def getArguments: util.Map[String, AnyRef] = arguments.map(arg => arg.name -> arg.serializableValue).toMap.asJava

    def hasProfilerStatistics: Boolean = arguments.exists(_.isInstanceOf[DbHits])

    def getName: String = name

    def getProfilerStatistics: ProfilerStatistics = new ProfilerStatistics {
      def getDbHits: Long = arguments.collectFirst { case DbHits(count) => count }.getOrElse(throw new InternalException("Don't have profiler stats"))
      def getRows: Long = arguments.collectFirst { case Rows(count) => count }.getOrElse(throw new InternalException("Don't have profiler stats"))
    }

    override def toString = self.toString
  }

  def addArgument(argument: Argument): PlanDescription = copy(arguments = arguments :+ argument)

  def map(f: PlanDescription => PlanDescription): PlanDescription = f(copy(children = children.map(f)))

  def toSeq: Seq[PlanDescription] = this +: children.toSeq

  override def toString = {
    val treeString = renderAsTree(this)
    val details = renderDetails(this)
    "%s%n%n%s".format(treeString, details)
  }

}

final case class NullPlanDescription(pipe:Pipe) extends PlanDescription {
  override def andThen(pipe: Pipe, name: String, arguments: Argument*) = new PlanDescriptionImpl(pipe, name, NoChildren, arguments)

  def args = Seq.empty

  def asJava = ???

  def children = NoChildren

  def find(searchedName: String) = if (searchedName == name) Seq(this) else Seq.empty

  def name = "Null"

  def render(builder: StringBuilder) {}

  def render(builder: StringBuilder, separator: String, levelSuffix: String) {}

  def addArgument(arg: Argument): PlanDescription =
    throw new UnsupportedOperationException("Cannot add arguments to NullPipe")

  // We do not map over this since we don't have profiler statistics for it
  def map(f: (PlanDescription) => PlanDescription): PlanDescription = this

  def arguments: Seq[Argument] = Seq.empty

  def toSeq: Seq[PlanDescription] = Seq(this)
}
