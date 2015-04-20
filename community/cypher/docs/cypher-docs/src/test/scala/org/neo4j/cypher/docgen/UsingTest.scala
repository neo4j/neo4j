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
package org.neo4j.cypher.docgen

import org.junit.Test

class UsingTest extends DocumentingTestBase {

  override def graphDescription = List(
    "Andres:Swedish KNOWS Peter",
    "Stefan:German KNOWS Andres",
    "Emil KNOWS Peter"
  )

  override val properties = Map(
    "Andres" -> Map("age" -> 36l, "awesome" -> true, "surname" -> "Taylor"),
    "Peter" -> Map("age" -> 34l),
    "Stefan" -> Map("surname" -> "Plantikow")
  )

  def section = "Using"

  @Test def query_using_single_index_hint() {
    db.createIndex("Swedish", "surname")

    testQuery(
      title = "Query using an index hint",
      text = "To query using an index hint, use +USING+ +INDEX+.",
      queryText = "match (n:Swedish) using index n:Swedish(surname) where n.surname = 'Taylor' return n",
      optionalResultExplanation = "The query result is returned as usual.",
      assertions = (p) => assert(p.toList === List(Map("n" -> node("Andres"))))
    )
  }

  @Test def query_using_multiple_index_hints() {
    db.createIndex("Swedish", "surname")
    db.createIndex("German", "surname")

    testQuery(
      title = "Query using multiple index hints",
      text = "To query using multiple index hints, use +USING+ +INDEX+.",
      queryText = "match (m:German)-->(n:Swedish) using index m:German(surname) using index n:Swedish(surname) where m.surname = 'Plantikow' and n.surname = 'Taylor' return m",
      optionalResultExplanation = "The query result is returned as usual.",
      assertions = (p) => assert(p.toList === List(Map("m" -> node("Stefan"))))
    )
  }

  @Test def query_forcing_label_scan() {
    testQuery(
      title = "Hinting a label scan",
      text = "If the best performance is to be had by scanning all nodes in a label and then filtering on that set, use +USING+ +SCAN+.",
      queryText = "match (m:German) using scan m:German where m.surname = 'Plantikow' return m",
      optionalResultExplanation = "This query does its work by finding all `:German` labeled nodes and filtering them by the surname property.",
      assertions = (p) => assert(p.toList === List(Map("m" -> node("Stefan"))))
    )
  }
}
