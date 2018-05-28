/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
