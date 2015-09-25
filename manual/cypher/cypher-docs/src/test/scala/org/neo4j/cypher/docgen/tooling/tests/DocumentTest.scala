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

import org.neo4j.cypher.docgen.tooling.Admonitions._
import org.neo4j.cypher.docgen.tooling._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class DocumentAsciiDocTest extends CypherFunSuite {
  test("Simplest possible document") {
    val doc = Document("title", "myId", initQueries = Seq.empty, Paragraph("lorem ipsum"))

    doc.asciiDoc should equal(
      """[[myId]]
        |= title
        |
        |lorem ipsum
        |
        |""".stripMargin)
  }

  test("Heading inside Document") {
    val doc = Document("title", "myId", initQueries = Seq.empty, Heading("My heading") ~ Paragraph("lorem ipsum"))

    doc.asciiDoc should equal(
      """[[myId]]
        |= title
        |
        |.My heading
        |lorem ipsum
        |
        |""".stripMargin)
  }

  test("Abstract for Document") {
    val doc = Document("title", "myId", initQueries = Seq.empty, Abstract("abstract intro"))

    doc.asciiDoc should equal(
      """[[myId]]
        |= title
        |
        |[abstract]
        |====
        |abstract intro
        |====
        |
        |""".stripMargin)
  }

  test("Section inside Section") {
    val doc = Document("title", "myId", initQueries = Seq.empty,
      Section("outer", Seq.empty,
        Paragraph("first") ~ Section("inner", Seq.empty, Paragraph("second"))
      ))

    doc.asciiDoc should equal(
      """[[myId]]
        |= title
        |
        |== outer
        |
        |first
        |
        |=== inner
        |
        |second
        |
        |""".stripMargin)
  }

  test("Tip with and without heading") {
    val doc = Document("title", "myId", initQueries = Seq.empty,
      Tip(Paragraph("tip text")) ~
        Tip("custom heading", Paragraph("tip text again"))
    )

    doc.asciiDoc should equal(
      """[[myId]]
        |= title
        |
        |[TIP]
        |====
        |tip text
        |
        |
        |====
        |
        |[TIP]
        |.custom heading
        |====
        |tip text again
        |
        |
        |====
        |
        |""".stripMargin)
  }

  test("Note with and without heading") {
    val doc = Document("title", "myId", initQueries = Seq.empty,
      Note(Paragraph("tip text")) ~
        Note("custom heading", Paragraph("tip text again"))
    )

    doc.asciiDoc should equal(
      """[[myId]]
        |= title
        |
        |[NOTE]
        |====
        |tip text
        |
        |
        |====
        |
        |[NOTE]
        |.custom heading
        |====
        |tip text again
        |
        |
        |====
        |
        |""".stripMargin)
  }

  test("Warning with and without heading") {
    val doc = Document("title", "myId", initQueries = Seq.empty,
      Warning(Paragraph("tip text")) ~
        Warning("custom heading", Paragraph("tip text again"))
    )

    doc.asciiDoc should equal(
      """[[myId]]
        |= title
        |
        |[WARNING]
        |====
        |tip text
        |
        |
        |====
        |
        |[WARNING]
        |.custom heading
        |====
        |tip text again
        |
        |
        |====
        |
        |""".stripMargin)
  }

  test("Caution with and without heading") {
    val doc = Document("title", "myId", initQueries = Seq.empty,
      Caution(Paragraph("tip text")) ~
        Caution("custom heading", Paragraph("tip text again"))
    )

    doc.asciiDoc should equal(
      """[[myId]]
        |= title
        |
        |[CAUTION]
        |====
        |tip text
        |
        |
        |====
        |
        |[CAUTION]
        |.custom heading
        |====
        |tip text again
        |
        |
        |====
        |
        |""".stripMargin)
  }

  test("Important with and without heading") {
    val doc = Document("title", "myId", initQueries = Seq.empty,
      Important(Paragraph("tip text")) ~
        Important("custom heading", Paragraph("tip text again"))
    )

    doc.asciiDoc should equal(
      """[[myId]]
        |= title
        |
        |[IMPORTANT]
        |====
        |tip text
        |
        |
        |====
        |
        |[IMPORTANT]
        |.custom heading
        |====
        |tip text again
        |
        |
        |====
        |
        |""".stripMargin)
  }

  test("QueryResult that creates data and returns nothing") {
    val doc = QueryResultTable(Seq(), Seq.empty, footer = "0 rows\nNodes created: 2\nRelationships created: 1\n")

    doc.asciiDoc(0) should equal(
      """.Result
        |[role="queryresult",options="footer",cols="1*<m"]
        ||===
        |1+|(empty result)
        |1+|0 rows
        |Nodes created: 2
        |Relationships created: 1
        |
        ||===
        |
        |""".stripMargin)
  }

  test("QueryResult that creates nothing and but returns data") {
    val doc = QueryResultTable(Seq("n1", "n2"), Seq(ResultRow(Seq("1", "2"))), footer = "1 row")

    doc.asciiDoc(0) should equal(
      """.Result
        |[role="queryresult",options="header,footer",cols="2*<m"]
        ||===
        ||n1|n2
        ||1|2
        |2+|1 row
        ||===
        |
        |""".stripMargin)
  }

  test("QueryResult that returns data containing pipes") {
    val doc = QueryResultTable(Seq("n1|x1", "n2"), Seq(ResultRow(Seq("1|2", "2"))), footer = "1 row")

    doc.asciiDoc(0) should equal(
      """.Result
        |[role="queryresult",options="header,footer",cols="2*<m"]
        ||===
        ||n1\|x1|n2
        ||1\|2|2
        |2+|1 row
        ||===
        |
        |""".stripMargin)
  }
}

class DocumentQueryTest extends CypherFunSuite {


  test("finds all queries and the init-queries they need") {
    val doc = Document("title", "myId", Seq("1"), Section("h1", Seq("2"),
      Section("h2", Seq("3"),
        Query("q", NoAssertions, Seq.empty, NoContent)
      ) ~ Query("q2", NoAssertions, Seq.empty, NoContent)
    ))

    doc.contentWithQueries should equal(Seq(
      ContentWithInit(Seq("1", "2", "3") , Query("q", NoAssertions, Seq.empty, NoContent)),
      ContentWithInit(Seq("1", "2"), Query("q2", NoAssertions, Seq.empty, NoContent)))
    )
  }

  test("Simplest possible document with a query in it") {
    val query = "match (n) return n"
    val doc = Document("title", "myId", initQueries = Seq.empty,
      Query(query, NoAssertions, Seq.empty, Paragraph("hello world")))

    val asciiDocResult = doc.asciiDoc
    asciiDocResult should equal(
      """[[myId]]
        |= title
        |
        |[source,cypher]
        |.Query
        |----
        |MATCH (n)
        |RETURN n
        |----
        |
        |hello world
        |
        |""".stripMargin)
  }
}
