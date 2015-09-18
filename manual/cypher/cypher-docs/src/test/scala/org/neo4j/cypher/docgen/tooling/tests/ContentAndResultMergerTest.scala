/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class ContentAndResultMergerTest extends CypherFunSuite {
  test("simple doc with query") {
    // given
    val query = "match (n) return n"
    val queryObj = Query(query, NoAssertions, QueryResultTable)
    val doc = Document("title", "myId", initQueries = Seq.empty, queryObj)

    val testResult = TestRunResult(Seq(QueryRunResult(queryObj, Right(Paragraph("14")))))

    // when
    val result = contentAndResultMerger(doc, testResult)

    // then
    result should equal(
      Document("title", "myId", initQueries = Seq.empty, Query(query, NoAssertions, Paragraph("14"))))
  }
}