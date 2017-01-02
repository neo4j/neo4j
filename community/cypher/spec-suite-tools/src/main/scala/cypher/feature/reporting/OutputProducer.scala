/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package cypher.feature.reporting

import java.util

import org.neo4j.cypher.internal.compiler.v3_2.ast.{QueryTagger, QueryTags}

import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap
import scala.collection.mutable

trait OutputProducer {

  def complete(query: String, outcome: Outcome): Unit
  def dump(): String
  def dumpTagStats: java.util.Map[String, Integer]
  def dumpTagCombinationStats: (java.util.List[java.util.List[java.lang.Integer]], java.util.List[String])
}

object JsonProducer extends JsonProducer(tagger = QueryTagger)

class JsonProducer(tagger: QueryTagger[String]) extends OutputProducer {

  override def complete(query: String, outcome: Outcome): Unit = {
    results += new JsonResult(query, tagger(query), outcome)
  }

  private val results = mutable.ListBuffer[JsonResult]()

  override def dump(): String = {
    import com.novus.salat._
    import com.novus.salat.global._

    grater[JsonResult].toPrettyJSONArray(results.toList)
  }

  override def dumpTagStats: java.util.Map[String, Integer] = {
    val tagCounts = results.map(result => result.prettyTags).foldLeft(Map.empty[String, Integer]) {
      case (map, tags) =>
        tags.foldLeft(map) {
          case (inner, tag) =>
            inner + ((tag, inner.getOrElse(tag, Int.box(0)) + 1))
        }
    }
    val withZeros = QueryTags.all.foldLeft(tagCounts) {
      case (map, tag) => if (map.contains(tag.toString)) map else map + ((tag.toString, 0))
    }

    sortByValue(withZeros).asJava
  }

  private def sortByValue(map: Map[String, Integer]) = ListMap(map.toList.sortBy(_._2): _*)

  override def dumpTagCombinationStats: (java.util.List[java.util.List[java.lang.Integer]], java.util.List[String]) = {
    val tags = QueryTags.all.toList.map(_.toString)
    val indexMap = tags.zipWithIndex.toMap

    val innerList = tags.indices.map(_ => Int.box(0))

    val list = new util.ArrayList[java.util.List[java.lang.Integer]]()
    tags.indices.foreach(_ => list.add(new util.ArrayList[java.lang.Integer](innerList.asJava)))

    results.map(result => result.prettyTags).foreach { tags =>
      // compile combinations
      val map: Set[(String, String)] = tags.flatMap(tag => tags.filterNot(s => s == tag).map(s => (tag, s)))
      map.foreach {
        case (t1, t2) if t1 != t2 =>
          val ints = list.get(indexMap(t1))
          ints.set(indexMap(t2), ints.get(indexMap(t2)) + 1)
        case _ =>
      }
    }
    (list, tags.asJava)
  }
}
