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
package org.neo4j.sunshine

import commands.Clause
import scala.collection.JavaConverters._
import org.apache.commons.lang.StringUtils
import pipes.Pipe
import org.neo4j.graphdb.{Relationship, NotFoundException, Node}
import org.neo4j.graphmatching.{PatternRelationship, PatternMatch, PatternNode, PatternMatcher}
import collection.immutable.Map


/**
 * Created by Andres Taylor
 * Date: 4/19/11
 * Time: 19:18 
 */

class Projection(pNodes: Map[String, PatternNode], pRels: Map[String, PatternRelationship], from: Pipe, select: Seq[Map[String, Any] => Map[String, Any]], filter: Clause) extends Traversable[Map[String, Any]] {
  def foreach[U](f: Map[String, Any] => U) {
    from.foreach((fromRow) => {

      fromRow.foreach((x) => {
        val variable: String = x._1
        val thingie: Any = x._2
        thingie match {
          case node: Node => pNodes(variable).setAssociation(node)
          case rel: Relationship => pRels(variable).setAssociation(rel)
        }

      })

      val startKey = pNodes.keys.head
      val startPNode = pNodes(startKey)
      val startNode = fromRow(startKey).asInstanceOf[Node]
      val patternMatches: java.lang.Iterable[PatternMatch] = PatternMatcher.getMatcher.`match`(startPNode, startNode)

      patternMatches.asScala.map((aMatch) => {
        val realResult: Map[String, Any] =
          pNodes.map((kv) => kv._1 -> aMatch.getNodeFor(kv._2)) ++
          pRels.map((kv) => kv._1 -> aMatch.getRelationshipFor(kv._2))

        if (filter.isMatch(realResult)) {
          val r = select.map((transformer) => transformer.apply(realResult)).reduceLeft(_ ++ _)

          f.apply(r)
        }
      })

    })
  }

  def columnAs[T](column: String): Iterator[T] = {
    this.map((map) => {
      val item: Any = map.getOrElse(column, throw new NotFoundException())
      item.asInstanceOf[T]
    }).toIterator
  }

  def javaIterator: java.util.Iterator[java.util.Map[String, Any]] = this.map((m) => m.asJava).toIterator.asJava

  //TODO: This is horrible. The Result object must know it's metadata in a state that doesn't force us to loop
  def calculateColumnSizes: Map[String, Int] = {
    val columnSizes = new scala.collection.mutable.HashMap[String, Int]

    this.foreach((m) => {
      m.foreach((kv) => {
        val length = kv._2.toString.size
        if (!columnSizes.contains(kv._1) || columnSizes.get(kv._1).get < length) {
          columnSizes.put(kv._1, length)
        }
      })
    })
    columnSizes.toMap
  }

  override def toString(): String = {
    val columnSizes = calculateColumnSizes
    val columns = columnSizes.keys.toList
    val builder = new StringBuilder()

    val headers = columns.map((c) => Map[String, Any](c -> c)).reduceLeft(_ ++ _)
    builder.append("| " + createString(columns, columnSizes, headers) + " |" + "\r\n")
    val wholeLine = StringUtils.repeat("-", builder.length - 2)
    builder.append(wholeLine + "\r\n")
    builder.insert(0, wholeLine + "\r\n")

    foreach((m) => {
      builder.append("| " + createString(columns, columnSizes, m) + " |" + "\r\n")
    })

    builder.append(wholeLine + "\r\n")

    builder.toString()
  }

  def createString(columns: List[String], columnSizes: Map[String, Int], m: Map[String, Any]): String = {
    columns.map((c) => {
      val length = columnSizes.get(c).get
      val value = StringUtils.rightPad(StringUtils.left(m.get(c).get.toString, length), length)
      value
    }).reduceLeft(_ + " | " + _)
  }

}