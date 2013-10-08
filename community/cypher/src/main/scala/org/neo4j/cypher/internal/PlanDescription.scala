/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal

import org.neo4j.cypher
import data.{MapVal, SimpleVal}
import helpers.StringRenderingSupport
import pipes.{NullPipe, Pipe}
import org.neo4j.cypher.javacompat.{PlanDescription => JPlanDescription, ProfilerStatistics}
import scala.collection.JavaConverters._
import java.util
import org.neo4j.cypher.ProfilerStatisticsNotReadyException

/**
 * Abstract description of an execution plan
 */
trait PlanDescription extends cypher.PlanDescription {
  def pipe: Pipe

  def args: Seq[(String, SimpleVal)]

  def children: Seq[PlanDescription]

  /**
   * @param pipe the pipe on which this PlanDescription is based on
   * @param name descriptive name of type of step
   * @param args optional arguments
   * @return a new PlanDescription that uses this as optional predecessor step description
   */
  def andThen(pipe: Pipe, name: String, args: (String, SimpleVal)*): PlanDescription

  def andThenWrap(pipe: Pipe, name: String, inner: PlanDescription, args: (String, SimpleVal)*): PlanDescription

  def mapArgs(f: (PlanDescription => Seq[(String, SimpleVal)])): PlanDescription

  def find(name: String): Option[PlanDescription]
}

class PlanDescriptionImpl(val pipe: Pipe,
                          val name: String,
                          val children: Seq[PlanDescription],
                          val args: Seq[(String, SimpleVal)]) extends PlanDescription with StringRenderingSupport {

  lazy val argsMap: MapVal = MapVal(args.toMap)

  def mapArgs(f: (PlanDescription => Seq[(String, SimpleVal)])): PlanDescription =
    new PlanDescriptionImpl(pipe, name, children.map(_.mapArgs(f)), f(this))

  def andThen(pipe: Pipe, name: String, args: (String, SimpleVal)*): PlanDescription =
    new PlanDescriptionImpl(pipe, name, Seq(this), args)

  def andThenWrap(pipe: Pipe, name: String,
                  inner: PlanDescription, args: (String, SimpleVal)*): PlanDescription =
    new PlanDescriptionImpl(pipe, name, Seq(inner), args)

  def withChildren(kids: PlanDescription*) =
    new PlanDescriptionImpl(pipe, name, kids, args)

  final override def render(builder: StringBuilder) {
    render(builder, "\n", "  ")
  }

  def render(builder: StringBuilder, separator: String, levelSuffix: String) {
    renderThis(builder)
    renderPrev(builder, separator, levelSuffix)
  }

  private def renderThis(builder: StringBuilder) {
    builder ++= name
    argsMap.render(builder, "(", ")", "()", escKey = false)
  }

  protected def renderPrev(builder: StringBuilder, separator: String, levelSuffix: String) {
    if (! children.isEmpty) {
      val newSeparator = separator + levelSuffix
      builder.append(separator)
      children.head.render(builder, newSeparator, levelSuffix)
      for (child <- children.tail) {
          builder.append(separator)
          child.render(builder, newSeparator, levelSuffix)
      }
    }
  }

  /**
   * Java-side PlanDescription implementation
   */
  private class PlanDescriptionConverter extends JPlanDescription {

    val _children: util.List[JPlanDescription] = children.map(_.asJava).toList.asJava

    val _args: util.Map[String, Object] = argsMap.asJava.asInstanceOf[java.util.Map[String, Object]]

    def cd(names: String*) = {
      var planDescription: JPlanDescription = this
      for (name <- names)
        planDescription = planDescription.getChild(name)
      planDescription
    }

    def getChild(name: String): JPlanDescription = {
      val iter = _children.iterator()
      while (iter.hasNext) {
        val child: JPlanDescription = iter.next()
        if (name.equals(child.getName))
          return child
      }
      throw new NoSuchElementException(name)
    }

    def getChildren = _children

    override val getName = name

    override val getArguments = _args

    val hasProfilerStatistics = hasLongArg("_rows") && hasLongArg("_db_hits")

    override lazy val getProfilerStatistics =
      if (hasProfilerStatistics) planStatisticsConverter else throw new ProfilerStatisticsNotReadyException()

    override def equals(obj: Any) = obj match {
      case obj: PlanDescriptionConverter if obj != null => PlanDescriptionImpl.this.equals(obj.asScala)
      case _ => false
    }

    override def toString = PlanDescriptionImpl.this.toString

    override def hashCode = PlanDescriptionImpl.this.hashCode()

    private def asScala = PlanDescriptionImpl.this

    private def hasLongArg(name: String) = _args.containsKey(name) && _args.get(name).isInstanceOf[Long]
  }

  private object planStatisticsConverter extends ProfilerStatistics {
    def getPlanDescription = PlanDescriptionImpl.this.asJava

    def getRows = getNamedLongStat("_rows")

    def getDbHits = getNamedLongStat("_db_hits")

    private def getNamedLongStat(name: String) =
     argsMap.v.get(name).getOrElse(throw new ProfilerStatisticsNotReadyException()).asJava.asInstanceOf[Long]
  }

  def find(name: String): Option[PlanDescription] =
    if (this.name == name)
      Some(this)
    else {
      children.head.find(name)
    }

  lazy val asJava: JPlanDescription = new PlanDescriptionConverter
}

object PlanDescription {
  /**
   * @param pipe pipe of this description
   * @param name desciptive name of type of step
   * @param args optional arguments
   * @return a new PlanDescription without an optional predecessor step description
   */
  def apply(pipe: Pipe, name: String, args: (String, SimpleVal)*) = new PlanDescriptionImpl(pipe, name, Seq.empty, args)
}

object NullPlanDescription extends PlanDescription {
  def andThen(pipe: Pipe, name: String, args: (String, SimpleVal)*) = PlanDescription(pipe, name, args: _*)

  def args = ???

  def asJava = ???

  def children = ???

  def find(name: String) = ???

  def mapArgs(f: (PlanDescription) => Seq[(String, SimpleVal)]) = ???

  def name = ???

  def pipe = NullPipe

  def render(builder: StringBuilder) {}

  def render(builder: StringBuilder, separator: String, levelSuffix: String) {}

  def andThen(inner: PlanDescription): PlanDescription = inner

  def andThenWrap(pipe: Pipe, name: String, inner: PlanDescription, args: (String, SimpleVal)*): PlanDescription = ???
}
