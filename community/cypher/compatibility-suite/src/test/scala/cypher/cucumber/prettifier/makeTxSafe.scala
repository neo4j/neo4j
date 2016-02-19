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

import cypher.feature.parser.EmptyNode
import org.neo4j.cypher.internal.helpers.GraphIcing
import org.neo4j.graphdb._
import org.neo4j.helpers.collection.{Iterables, IteratorUtil}
import org.neo4j.kernel.GraphDatabaseQueryService

import scala.collection.JavaConverters._

object makeTxSafe extends ((GraphDatabaseQueryService, Result) => util.List[util.Map[String, AnyRef]]) with GraphIcing {

  override def apply(graph: GraphDatabaseQueryService, raw: Result): util.List[util.Map[String, AnyRef]] = {
    val safe = graph.inTx {
      replaceNodes(IteratorUtil.asList(raw))
    }
    raw.close()
    safe
  }

  def replaceNodes(rows: util.List[util.Map[String, AnyRef]]) = {
    val scalaResults = rows.asScala.map(_.asScala)
    scalaResults.flatMap { map =>
      map.map { case (key, value) =>
        Map(key -> convert(value))
      }
    }.map(_.asJava).asJava
  }


  def convert(original: AnyRef): AnyRef = original match {
    case n: Node => EmptyNode.newWith(Iterables.toList(n.getLabels), n.getAllProperties)
    case r: Relationship => null
    case p: Path => null
    case x => x
  }
}
