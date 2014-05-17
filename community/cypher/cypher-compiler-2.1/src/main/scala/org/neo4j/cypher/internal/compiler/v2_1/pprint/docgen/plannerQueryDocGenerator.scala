/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.pprint.docgen

import org.neo4j.cypher.internal.compiler.v2_1.pprint.{Doc, DocGenerator, RecursiveDocGenerator, NestedDocGenerator}
import org.neo4j.cypher.internal.compiler.v2_1.planner.PlannerQuery
import scala.annotation.tailrec

case object plannerQueryDocGenerator extends NestedDocGenerator[Any] {

  import Doc._

  val instance: RecursiveDocGenerator[Any] = {
    case plannerQuery: PlannerQuery => (inner: DocGenerator[Any]) =>
      val allQueryDocs = queryDocs(inner, Some(plannerQuery), List.empty)
      group(breakList(allQueryDocs))
  }

  @tailrec
  private def queryDocs(inner: DocGenerator[Any], optQuery: Option[PlannerQuery], docs: List[Doc]): List[Doc] = {
    optQuery match {
      case None => docs.reverse
      case Some(query) => queryDocs(inner, query.tail, queryDoc(inner, query) :: docs)
    }
  }

  private def queryDoc(inner: DocGenerator[Any], query: PlannerQuery) = {
    val graphDoc = inner(query.graph)
    val projectionPrefix = query.tail.map(_ => "WITH").getOrElse("RETURN")
    val projectionDoc = queryProjectionDocGenerator(projectionPrefix)(query.projection)(inner)
    group(graphDoc :/: projectionDoc)
  }
}
