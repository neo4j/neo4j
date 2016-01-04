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
package org.neo4j.cypher.docgen.tooling.tests

import org.neo4j.cypher.docgen.tooling._
import org.neo4j.cypher.internal.frontend.v3_0.test_helpers.CypherFunSuite

class RunnableContentTest extends CypherFunSuite {
  test("graph viz includes all init queries, and the actual query when inside a Query object") {
    val graphVizPlaceHolder = new GraphVizPlaceHolder("")
    val tablePlaceHolder = new TablePlaceHolder(NoAssertions)
    val queryObject = Query("5", NoAssertions, Seq("3", "4"), graphVizPlaceHolder ~ tablePlaceHolder)
    val doc = Document("title", "id", Seq("1","2"), queryObject)

    doc.contentWithQueries should equal(Seq(
      ContentWithInit(Seq("1", "2", "3", "4", "5"), graphVizPlaceHolder),
      ContentWithInit(Seq("1", "2", "3", "4", "5"), tablePlaceHolder)
    ))
  }
}
