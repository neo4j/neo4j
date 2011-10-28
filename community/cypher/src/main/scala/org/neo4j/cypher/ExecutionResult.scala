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

package org.neo4j.cypher

import scala.collection.JavaConverters._
import org.neo4j.graphdb.{PropertyContainer, Relationship, NotFoundException, Node}
import collection.Traversable


trait ExecutionResult extends Traversable[Map[String, Any]] with StringExtras {
  val symbols: SymbolTable

  val columns: List[String]

  def javaColumns: java.util.List[String] = columns.asJava

  def javaColumnAs[T](column: String) = columnAs[T](column).map(x=>makeValueJavaCompatible(x).asInstanceOf[T]).asJava

  def columnAs[T](column: String): Iterator[T] = {
    this.map(m => {
      val item: Any = m.getOrElse(column, throw new NotFoundException("No column named '" + column + "' was found."))
      item.asInstanceOf[T]
    }).toIterator
  }

  def makeValueJavaCompatible(value: Any): Any = value match {
    case iter: Seq[_] => iter.asJava
    case x => x
  }

  def javaIterator: java.util.Iterator[java.util.Map[String, Any]] = this.map(m => {
    m.map(kv => kv._1 -> makeValueJavaCompatible(kv._2)).asJava
  }).toIterator.asJava

  def calculateColumnSizes(result: Seq[Map[String, Any]]): Map[String, Int] = {
    val columnSizes = new scala.collection.mutable.HashMap[String, Int] ++ columns.map(name => name -> name.size)

    result.foreach((m) => {
      m.foreach((kv) => {
        val length = text(kv._2).size
        if (!columnSizes.contains(kv._1) || columnSizes.get(kv._1).get < length) {
          columnSizes.put(kv._1, length)
        }
      })
    })
    columnSizes.toMap
  }

  def dumpToString(): String = {
    val start = System.currentTimeMillis()
    val result = this.toList
    val timeTaken = System.currentTimeMillis() - start

    val columnSizes = calculateColumnSizes(result)

    val headers = columns.map((c) => Map[String, Any](c -> c)).reduceLeft(_ ++ _)
    val headerLine: String = createString(columns, columnSizes, headers)
    val lineWidth: Int = headerLine.length - 2
    val --- = "+" + repeat("-", lineWidth) + "+"

    val resultLines: Traversable[String] = result.map(createString(columns, columnSizes, _))

    val footer = "%d rows, %d ms".format(resultLines.size, timeTaken)


    val lines = List(
      ---,
      headerLine,
      ---) ++
      resultLines ++
      List(---)

    lines.mkString("\r\n") + "\r\n" + footer
  }


  def props(x: PropertyContainer): String = x.getPropertyKeys.asScala.map(key => key + "->" + quoteString(x.getProperty(key))).mkString("{", ",", "}")

  def text(obj: Any): String = obj match {
    case x: Node => x.toString + props(x)
    case x: Relationship => ":" + x.getType.toString + "[" + x.getId + "] " + props(x)
    case null => "<null>"
    case x => x.toString
  }

  def quoteString(in: Any): String = in match {
    case x: String => "\"" + x + "\""
    case x => x.toString
  }

  def createString(columns: List[String], columnSizes: Map[String, Int], m: Map[String, Any]): String = {
    columns.map(c => {
      val length = columnSizes.get(c).get
      val txt = text(m.get(c).get)
      val value = makeSize(txt, length)
      value
    }).mkString("| ", " | ", " |")
  }
}