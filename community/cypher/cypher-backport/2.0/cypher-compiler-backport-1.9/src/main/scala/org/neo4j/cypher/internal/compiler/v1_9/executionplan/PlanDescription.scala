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
package org.neo4j.cypher.internal.compiler.v1_9.executionplan

import scala.collection.JavaConverters._
import java.util
import org.neo4j.cypher.internal.compiler.v1_9.pipes.Pipe
import org.neo4j.cypher.internal.compiler.v1_9.data.{MapVal, SimpleVal}
import org.neo4j.cypher.internal.helpers.StringRenderingSupport
import org.neo4j.cypher.javacompat.ProfilerStatistics
import org.neo4j.cypher.ProfilerStatisticsNotReadyException
import org.neo4j.cypher.javacompat.{PlanDescription => JPlanDescription}
import org.neo4j.cypher

/**
 * Abstract description of an execution plan
 *
 * @param name descriptive name of type of step
 * @param children optional predecessor step description
 * @param args optional arguments
 */
class PlanDescription(val pipe: Pipe,
                      val name: String,
                      val children: Seq[PlanDescription],
                      val args: Seq[(String, SimpleVal)]) extends cypher.PlanDescription with StringRenderingSupport {

  lazy val argsMap: MapVal = MapVal(args.toMap)

  def mapArgs(f: (PlanDescription => Seq[(String, SimpleVal)])): PlanDescription =
    new PlanDescription(pipe, name, children.map( _.mapArgs(f) ), f(this))

  /**
   * @param pipe the pipe on which this PlanDescription is based on
   * @param name descriptive name of type of step
   * @param args optional arguments
   * @return a new PlanDescription that uses this as optional predecessor step description
   */
  def andThen(pipe: Pipe, name: String, args: (String, SimpleVal)*) =
    new PlanDescription(pipe, name, Seq(this), args)

  def withChildren(kids: PlanDescription*) =
    new PlanDescription(pipe, name, kids, args)
  /**
   * Render this plan description and all predecessor step descriptions to builder using the default separator
   *
   * @param builder StringBuilder to be used
   */
  final override def render(builder: StringBuilder) {
    render(builder, "\n", "  ")
  }

  /**
   * Render this plan description and all predecessor step descriptions to builder
   *
   * @param builder StringBuilder to be used
   * @param separator separator to be inserted between predecessor step descriptions
   * @param levelSuffix separator suffix to be added per child nesting level
   */
  def render(builder: StringBuilder, separator: String, levelSuffix: String) {
    renderThis(builder)
    renderPrev(builder, separator, levelSuffix)
  }

  /**
   * Render this plan description (ignoring predecessor step descriptions) to builder
   *
   * @param builder StringBuilder to be used
   */
  def renderThis(builder: StringBuilder) {
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
      case obj: PlanDescriptionConverter if obj != null => PlanDescription.this.equals(obj.asScala)
      case _ => false
    }

    override def toString = PlanDescription.this.toString

    override def hashCode = PlanDescription.this.hashCode()

    private def asScala = PlanDescription.this

    private def hasLongArg(name: String) = _args.containsKey(name) && _args.get(name).isInstanceOf[Long]
  }

  private object planStatisticsConverter extends ProfilerStatistics {
    def getPlanDescription = PlanDescription.this.asJava

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
  def apply(pipe: Pipe, name: String, args: (String, SimpleVal)*) = new PlanDescription(pipe, name, Seq.empty, args)
}


