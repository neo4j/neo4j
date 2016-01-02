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

class ContentAndResultMergerTest extends CypherFunSuite {

  val GRAPHVIZ_RESULT = GraphViz("APA")
  val TABLE_RESULT = Paragraph("14")
  val QUERY = "match (n) return n"
  val TABLE_PLACEHOLDER = new TablePlaceHolder(NoAssertions)
  val GRAPHVIZ_PLACEHOLDER = new GraphVizPlaceHolder("")

  test("simple doc with query") {
    // given
    val doc = Document("title", "myId", initQueries = Seq.empty, TABLE_PLACEHOLDER)

    val testResult = TestRunResult(Seq(QueryRunResult(QUERY, TABLE_PLACEHOLDER, Right(TABLE_RESULT))))

    // when
    val result = contentAndResultMerger(doc, testResult)

    // then
    result should equal(
      Document("title", "myId", initQueries = Seq.empty, TABLE_RESULT))
  }

  test("simple doc with GraphVizBefore") {
    // given
    val doc = Document("title", "myId", initQueries = Seq.empty, GRAPHVIZ_PLACEHOLDER)

    val testResult = TestRunResult(Seq(GraphVizRunResult(GRAPHVIZ_PLACEHOLDER, GRAPHVIZ_RESULT)))

    // when
    val result = contentAndResultMerger(doc, testResult)

    // then
    result should equal(
      Document("title", "myId", initQueries = Seq.empty, GRAPHVIZ_RESULT))
  }

  test("doc with GraphVizBefore and Result Table without Query") {
    // given
    val doc = Document("title", "myId", initQueries = Seq.empty, GRAPHVIZ_PLACEHOLDER ~ TABLE_PLACEHOLDER)

    val testResult = TestRunResult(Seq(
      GraphVizRunResult(GRAPHVIZ_PLACEHOLDER, GRAPHVIZ_RESULT),
      QueryRunResult(QUERY, TABLE_PLACEHOLDER, Right(TABLE_RESULT))
    ))

    // when
    val result = contentAndResultMerger(doc, testResult)

    // then
    result should equal(
      Document("title", "myId", initQueries = Seq.empty, GRAPHVIZ_RESULT ~ TABLE_RESULT))
  }

  test("doc with GraphVizBefore and Result Table inside of Query") {
    // given
    val queryObj = Query(QUERY, NoAssertions, Seq.empty, TABLE_PLACEHOLDER ~ GRAPHVIZ_PLACEHOLDER)
    val doc = Document("title", "myId", initQueries = Seq.empty, queryObj)

    val testResult = TestRunResult(Seq(
      QueryRunResult(QUERY, TABLE_PLACEHOLDER, Right(TABLE_RESULT)),
      GraphVizRunResult(GRAPHVIZ_PLACEHOLDER, GRAPHVIZ_RESULT)
    ))

    // when
    val result = contentAndResultMerger(doc, testResult)

    // then
    result should equal(
      Document("title", "myId", initQueries = Seq.empty, Query(QUERY, NoAssertions, Seq.empty, TABLE_RESULT ~ GRAPHVIZ_RESULT)))
  }
}
