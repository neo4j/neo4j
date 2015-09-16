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

import org.neo4j.cypher.docgen.cookbook._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class DocumentTest extends CypherFunSuite {
  test("Document containing no queries produces no tests") {
    val doc = Document("title", initQueries = Seq.empty, Paragraph("lorem ipsum"))

    doc.tests should be(empty)
  }

  test("Document containing a query produces a test") {
    val doc = Document("title", initQueries = Seq.empty, Query("MATCH n RETURN n", NoAssertions, QueryResultTable))

    doc.tests.toList should be(Seq("MATCH n RETURN n" -> NoAssertions))
  }
}
