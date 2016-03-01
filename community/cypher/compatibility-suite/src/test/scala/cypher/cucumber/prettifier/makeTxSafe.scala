/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package cypher.cucumber.prettifier

import java.util

import cypher.feature.parser.{ParsedNode, ParsedRelationship}
import org.neo4j.cypher.internal.helpers.GraphIcing
import org.neo4j.graphdb._
import org.neo4j.helpers.collection.IteratorUtil
import org.neo4j.kernel.GraphDatabaseQueryService

import scala.collection.JavaConverters._
import scala.collection.mutable

object makeTxSafe extends ((GraphDatabaseQueryService, Result) => util.List[util.Map[String, AnyRef]]) with GraphIcing {

  override def apply(graph: GraphDatabaseQueryService, raw: Result): util.List[util.Map[String, AnyRef]] = {
    val asList = IteratorUtil.asList(raw)
    val scalaItr = asList.asScala.map(_.asScala)
    graph.inTx {
      replaceNodes(scalaItr)
    }
  }

  def replaceNodes(rows: mutable.Seq[mutable.Map[String, AnyRef]]) = {
    val newList = new util.ArrayList[util.Map[String, AnyRef]]()
    rows.foreach((map: mutable.Map[String, AnyRef]) => {
      val newMap = new util.HashMap[String, AnyRef]()
      map.foreach { case (key, value) =>
        newMap.put(key, convert(value))
      }
      newList.add(newMap)
    })
    newList
  }

  def convert(original: AnyRef): AnyRef = original match {
    case n: Node => ParsedNode.fromRealNode(n)
    case r: Relationship => ParsedRelationship.fromRealRelationship(r)
    case p: Path => null
    case l: util.List[AnyRef] =>
      val newList = new util.ArrayList[AnyRef]()
      l.asScala.foreach { e =>
        newList.add(convert(e))
      }
      newList
    case x => x
  }
}
