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
package org.neo4j.cypher

import internal.helpers.IsCollection
import internal.pipes.Pipe
import javacompat.{PlanDescription => JPlanDescription, ProfilerStatistics}
import scala.collection.JavaConverters._
import java.util

/**
 * Abstract description of an execution plan
 *
 * @param name descriptive name of type of step
 * @param children optional predecessor step description
 * @param args optional arguments
 */
class PlanDescription(val pipe: Pipe,
                      val name: String,
                      val parent: Option[PlanDescription],
                      val children: Seq[PlanDescription],
                      val args: Seq[(String, Any)]) {

  lazy val argsMap: Map[String, Any] = args.toMap

  def mapArgs(f: (PlanDescription => Seq[(String, Any)])): PlanDescription =
    new PlanDescription(pipe, name, parent, children.map( _.mapArgs(f) ), f(this))

  /**
   * @param pipe the pipe on which this PlanDescription is based on
   * @param name descriptive name of type of step
   * @param args optional arguments
   * @return a new PlanDescription that uses this as optional predecessor step description
   */
  def andThen(pipe: Pipe, name: String, args: (String, Any)*) =
    new PlanDescription(pipe, name, Some(this), Seq(this), args)

  /**
   * Render this plan description and all predecessor step descriptions to builder
   *
   * @param builder StringBuilder to be used
   * @param separator separator to be inserted between predecessor step descriptions
   */
  def renderAll(builder: StringBuilder, separator: String = "\n") {
    renderThis(builder)
    renderPrev(builder, separator)
  }

  /**
   * Render this plan description (ignoring predecessor step descriptions) to builder
   *
   * @param builder StringBuilder to be used
   */
  def renderThis(builder: StringBuilder) {
    builder ++= name
    builder += '('
    renderArgs(builder: StringBuilder, args)
    builder += ')'
  }

  protected def renderPrev(builder: StringBuilder, separator: String) {
    children match {
      case Seq() =>
      case Seq(source) =>
        builder.append(separator)
        source.renderAll(builder, separator)
      case children: Seq[PlanDescription] =>
        builder ++= " {"
        val newSeparator = separator + "  "
        for (child <- children) {
          builder.append(newSeparator)
          child.renderAll(builder, newSeparator)
        }
        builder.append(separator)
        builder ++= "} "
    }
  }

  protected def renderArgs(builder: StringBuilder, args: Seq[(String, Any)]) {
    for ((name, value) <- args.headOption) {
      renderPair(builder, name, value)
      for ((name, value) <- args.tail) {
        builder += ','
        builder += ' '
        renderPair(builder, name, value)
      }
    }
  }

  protected def renderPair(builder: StringBuilder, name: String, value: Any) {
    builder ++= name
    builder += '='
    value match {
      case _: String =>
        builder += '"'
        builder ++= value.toString
        builder += '"'
      case IsCollection(coll) =>
        builder += '['
        builder ++= coll.map(stringify(_)).mkString(",")
        builder += ']'
      case _ =>
        builder ++= value.toString
    }
  }

  private def stringify(v: Any) = new {
    override def toString = v match {
      case v: String => '"' + v.toString + '"'
      case v: Any    => v.toString
    }
  }

  override def toString = {
    val builder = new StringBuilder
    renderAll(builder)
    builder.toString()
  }

  lazy val asJava: JPlanDescription = new PlanDescriptionConverter

  /**
   * Java-side PlanDescription implementation
   */
  private class PlanDescriptionConverter extends JPlanDescription {

    val _children = children.map(_.asJava).toList.asJava

    val _args: util.Map[String, Object] = argsMap.asInstanceOf[Map[String, AnyRef]].asJava

    def isRoot = parent.isEmpty

    override lazy val getRoot = if (isRoot) this else getParent.getRoot

    def getParent = parent.getOrElse(throw new NoSuchElementException("Root does not have a parent")).asJava

    def cd(names: String*) = {
      var planDescription: JPlanDescription = this
      for (name <- names)
        planDescription = planDescription.getChild(name)
      planDescription
    }

    def getChild(name: String) = {
      val optChild = children.find(name == _.asJava.getName)
      optChild.getOrElse(throw new NoSuchElementException(name)).asJava
    }

    def getChildren = _children

    override val getName = name

    override val getArguments = _args

    def hasProfilerStatistics = hasIntArg("_rows") && hasIntArg("_db_hits")

    override val getProfilerStatistics =
      if (hasProfilerStatistics) planStatisticsConverter else throw new ProfilerStatisticsNotReadyException()

    override def equals(obj: Any) = obj match {
      case obj: PlanDescriptionConverter if obj != null => PlanDescription.this.equals(obj.asScala)
      case _ => false
    }

    override def toString = PlanDescription.this.toString

    override def hashCode = PlanDescription.this.hashCode()

    private def asScala = PlanDescription.this

    private def hasIntArg(name: String) = argsMap.contains(name) && argsMap(name).isInstanceOf[Int]
  }

  private object planStatisticsConverter extends ProfilerStatistics {
    def getPlanDescription = PlanDescription.this.asJava

    def getRows = getNamedIntStat("_rows")

    def getDbHits = getNamedIntStat("_db_hits")

    private def getNamedIntStat(name: String) =
      argsMap.get(name).getOrElse(throw new ProfilerStatisticsNotReadyException()).asInstanceOf[Int]
  }
}

object PlanDescription {
  /**
   * @param pipe pipe of this description
   * @param name desciptive name of type of step
   * @param args optional arguments
   * @return a new PlanDescription without an optional predecessor step description
   */
  def apply(pipe: Pipe, name: String, args: (String, Any)*) = new PlanDescription(pipe, name, None, Seq.empty, args)
}



