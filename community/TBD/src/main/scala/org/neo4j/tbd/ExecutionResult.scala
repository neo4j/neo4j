/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
/**
 *  opyright (c)


2-1" eo T ch o y
 * Netw rEn
neo
bject  in Lun  AB [ht p://neo echnology.com]
 *
 * Th s file is part of Neo4j.
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
package org.neo4j.tbd

import scala.collection.JavaConverters._
import org.neo4j.graphdb.{PropertyContainer, Relationship, NotFoundException, Node}
import org.apache.commons.lang.StringUtils

trait ExecutionResult extends Traversable[Map[String, Any]]
{
  def javaColumnAs[T](column: String) = columnAs[T](column).asJava

  def columnAs[T](column: String): Iterator[T] =
  {
    this.map((map) =>
    {
      val item: Any = map.getOrElse(column, throw new NotFoundException("No column named '" + column + "' was found."))
      item.asInstanceOf[T]
    }).toIterator
  }

  def javaIterator: java.util.Iterator[java.util.Map[String, Any]] = this.map((m) => m.asJava).toIterator.asJava

  def calculateColumnSizes: Map[String, Int] =
  {
    val columnSizes = new scala.collection.mutable.HashMap[String, Int]

    this.foreach((m) =>
    {
      m.foreach((kv) =>
      {
        val length = text(kv._2).size
        if ( !columnSizes.contains(kv._1) || columnSizes.get(kv._1).get < length )
        {
          columnSizes.put(kv._1, length)
        }
      })
    })
    columnSizes.toMap
  }

  def dumpToString(): String =
  {
    val columnSizes = calculateColumnSizes
    val columns = columnSizes.keys.toList
    val builder = new StringBuilder()

    val headers = columns.map((c) => Map[String, Any](c -> c)).reduceLeft(_ ++ _)
    builder.append("| " + createString(columns, columnSizes, headers) + " |" + "\r\n")
    val wholeLine = StringUtils.repeat("-", builder.length - 2)
    builder.append(wholeLine + "\r\n")
    builder.insert(0, wholeLine + "\r\n")

    foreach((m) =>
    {
      builder.append("| " + createString(columns, columnSizes, m) + " |" + "\r\n")
    })

    builder.append(wholeLine + "\r\n")

    builder.toString()
  }

  def props(x: PropertyContainer): String = x.getPropertyKeys.asScala.map((key) => key + "->" + quoteString(x.getProperty(key))).mkString("{", ",", "}")

  def text(obj: Any): String = obj match
  {
    case x: Node => x.toString + props(x)
    case x: Relationship => ":" + x.getType.toString + "[" + x.getId + "] " + props(x)
    case null => "<null>"
    case x => x.toString
  }

  def quoteString(in: Any): String = in match
  {
    case x: String => "\"" + x + "\""
    case x => x.toString
  }

  def createString(columns: List[String], columnSizes: Map[String, Int], m: Map[String, Any]): String =
  {
    columns.map((c) =>
    {
      val length = columnSizes.get(c).get
      val value = StringUtils.rightPad(StringUtils.left(text(m.get(c).get), length), length)
      value
    }).reduceLeft(_ + " | " + _)
  }

}