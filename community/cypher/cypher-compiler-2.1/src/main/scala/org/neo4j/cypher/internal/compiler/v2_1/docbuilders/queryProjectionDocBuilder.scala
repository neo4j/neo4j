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
package org.neo4j.cypher.internal.compiler.v2_1.docbuilders

import org.neo4j.cypher.internal.compiler.v2_1.perty._
import org.neo4j.cypher.internal.compiler.v2_1.planner.{AggregatingQueryProjection, QueryProjection}
import org.neo4j.cypher.internal.compiler.v2_1.ast.Expression

case class queryProjectionDocBuilder(prefix: String = "WITH") extends CachingDocBuilder[Any] {

  import Doc._

  override protected def newNestedDocGenerator = {
    case queryProjection: AggregatingQueryProjection =>
      val distinct = if (queryProjection.aggregationExpressions.isEmpty) "DISTINCT" else ""
      generateDoc(queryProjection, queryProjection.projections ++ queryProjection.aggregationExpressions, distinct)

    case queryProjection: QueryProjection =>
      generateDoc(queryProjection, queryProjection.projections)
  }

  private def generateDoc(queryProjection: QueryProjection,
                          projectionsMap: Map[String, Expression],
                          initialString: String = ""): DocGenerator[Any] => Doc =
  { (inner: DocGenerator[Any]) =>

      val projectionMapDoc = projectionsMap.collect {
        case (k, v) => group(inner(v) :/: "AS " :: s"`$k`")
      }

      val projectionDoc = if (projectionMapDoc.isEmpty) text("*") else group(sepList(projectionMapDoc))
      val shuffleDoc = inner(queryProjection.shuffle)

      section(prefix, projectionDoc :+: shuffleDoc)
  }
}
