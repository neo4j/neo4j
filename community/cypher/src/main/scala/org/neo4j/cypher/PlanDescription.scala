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

/**
 * Abstract description of an execution plan
 *
 * @param name descriptive name of type of step
 * @param optPrev optional predecessor step description
 * @param args optional arguments
 */
class PlanDescription(name: String, optPrev: Option[PlanDescription], args: Seq[(String, Any)]) {

  /**
   * @param name desciptive name of type of step
   * @param args optional arguments
   * @return a new PlanDescription that uses this as optional predecessor step description
   */
  def andThen(name: String, args: (String, Any)*) = new PlanDescription(name, Some(this), args)

  /**
   * @return list of this plan description and all its predecessor step descriptions
   */
  def toList: List[PlanDescription] = optPrev match {
    case Some(prev) => this :: prev.toList
    case _          => List.empty
  }

  /**
   * Render this plan description and all predecessor step descriptions to builder
   *
   * @param builder StringBuilder to be used
   * @param separator separator to be inserted between predecessor step descriptions
   */
  def renderAll(builder: StringBuilder, separator: String = " <= ") {
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
    optPrev match {
      case Some(source) =>
        builder.append(separator)
        source.renderAll(builder, separator)
      case _ =>
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
}

object PlanDescription {
  /**
   * @param name desciptive name of type of step
   * @param args optional arguments
   * @return a new PlanDescription without an optional predecessor step description
   */
  def apply(name: String, args: (String, Any)*) = new PlanDescription(name, None, args)
}


