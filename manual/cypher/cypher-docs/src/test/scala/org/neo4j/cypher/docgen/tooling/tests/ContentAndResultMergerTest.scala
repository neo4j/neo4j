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

  val GRAPHVIZ_RESULT = GraphViz("APA")
  val QUERY_RESULT = Paragraph("14")
  val QUERY = "match (n) return n"
  val QUERY_CONTENT = Query(QUERY, NoAssertions, Seq.empty, QueryResultTablePlaceholder)
  val GRAPHVIZ_BEFORE = new GraphVizPlaceHolder

  test("simple doc with query") {
    // given
    val doc = Document("title", "myId", initQueries = Seq.empty, QUERY_CONTENT)

    val testResult = TestRunResult(Seq(QueryRunResult(QUERY, QUERY_CONTENT, Right(QUERY_RESULT))))

    // when
    val result = contentAndResultMerger(doc, testResult)

    // then
    result should equal(
      Document("title", "myId", initQueries = Seq.empty, Query(QUERY, NoAssertions, Seq.empty, QUERY_RESULT)))
  }

  test("simple doc with GraphVizBefore") {
    // given
    val doc = Document("title", "myId", initQueries = Seq.empty, GRAPHVIZ_BEFORE)

    val testResult = TestRunResult(Seq(GraphVizRunResult(GRAPHVIZ_BEFORE, GRAPHVIZ_RESULT)))

    // when
    val result = contentAndResultMerger(doc, testResult)

    // then
    result should equal(
      Document("title", "myId", initQueries = Seq.empty, GRAPHVIZ_RESULT))
  }

  test("doc with GraphVizBefore and Query") {
    // given
    val doc = Document("title", "myId", initQueries = Seq.empty, GRAPHVIZ_BEFORE ~ QUERY_CONTENT)

    val testResult = TestRunResult(Seq(
      GraphVizRunResult(GRAPHVIZ_BEFORE, GRAPHVIZ_RESULT),
      QueryRunResult(QUERY, QUERY_CONTENT, Right(QUERY_RESULT))
    ))

    // when
    val result = contentAndResultMerger(doc, testResult)

    // then
    result should equal(
      Document("title", "myId", initQueries = Seq.empty, GRAPHVIZ_RESULT ~ Query(QUERY, NoAssertions, Seq.empty, QUERY_RESULT)))
  }

  test("doc with GraphVizBefore inside of Query") {
    // given
    val queryObj = Query(QUERY, NoAssertions, Seq.empty, QueryResultTablePlaceholder ~ GRAPHVIZ_BEFORE)
    val doc = Document("title", "myId", initQueries = Seq.empty, queryObj)

    val testResult = TestRunResult(Seq(
      GraphVizRunResult(GRAPHVIZ_BEFORE, GRAPHVIZ_RESULT),
      QueryRunResult(QUERY, queryObj, Right(QUERY_RESULT))
    ))

    // when
    val result = contentAndResultMerger(doc, testResult)

    // then
    result should equal(
      Document("title", "myId", initQueries = Seq.empty, Query(QUERY, NoAssertions, Seq.empty, QUERY_RESULT ~ GRAPHVIZ_RESULT)))
  }
}
