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
package org.neo4j.cypher.docgen.tooling

import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class DocumentTest extends CypherFunSuite {
  test("Simplest possible document") {
    val doc = Document("title", "myId", initQueries = Seq.empty, Paragraph("lorem ipsum"))

    doc.tests should be(empty)
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

    doc.tests should be(empty)
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

    doc.tests should be(empty)
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
      Section("outer",
        Paragraph("first") ~ Section("inner", Paragraph("second"))
      ))

    doc.tests should be(empty)
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

    doc.tests should be(empty)
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

  test("Document containing a query produces a test") {
    val doc = Document("title", "myId", initQueries = Seq.empty, Query("MATCH n RETURN n", NoAssertions, QueryResultTable))

    doc.tests.toList should be(Seq("MATCH n RETURN n" -> NoAssertions))
  }
}
