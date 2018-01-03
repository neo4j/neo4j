/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import org.junit.Assert.assertThat
import org.hamcrest.Matchers._

class UsingTest extends DocumentingTestBase {

  override def graphDescription = List(
    "Andres:Swede KNOWS Peter",
    "Stefan:German KNOWS Andres",
    "Stefan KNOWS Peter",
    "Emil:Expat KNOWS Peter",
    "Emil KNOWS Andres",
    "Emil KNOWS Stefan",
    "Jim:Brit KNOWS Andres",
    "Jim KNOWS Peter",
    "Jim KNOWS Emil",
    "Jim KNOWS Stefan"
  )

  override val setupConstraintQueries: List[String] = List("CREATE INDEX ON :Swede(surname)", "CREATE INDEX ON :German(surname)")

  override val properties = Map(
    "Andres" -> Map[String, Any]("age" -> 40l, "surname" -> "Taylor"),
    "Peter" -> Map[String, Any]("age" -> 42l, "surname" -> "Neubauer"),
    "Stefan" -> Map[String, Any]("age" -> 37l, "surname" -> "Plantikow"),
    "Emil" -> Map[String, Any]("age" -> 37l, "surname" -> "Eifrem"),
    "Jim" -> Map[String, Any]("age" -> 39l, "surname" -> "Webber")
  )

  def section = "Using"

  @Test def query_using_single_index_hint() {
    profileQuery(
      title = "Query using an index hint",
      text = "To query using an index hint, use +USING+ +INDEX+.",
      queryText = "match (n:Swede) using index n:Swede(surname) where n.surname = 'Taylor' return n",
      assertions = (p) => assert(p.toList === List(Map("n" -> node("Andres"))))
    )
  }

  @Test def query_using_multiple_index_hints() {
    profileQuery(
      title = "Query using multiple index hints",
      text = "To query using multiple index hints, use +USING+ +INDEX+.",
      queryText = "match (m:German)-->(n:Swede) using index m:German(surname) using index n:Swede(surname) where m.surname = 'Plantikow' and n.surname = 'Taylor' return m",
      assertions = (p) => assert(p.toList === List(Map("m" -> node("Stefan"))))
    )
  }

  @Test def query_forcing_label_scan() {
    profileQuery(
      title = "Hinting a label scan",
      text = "If the best performance is to be had by scanning all nodes in a label and then filtering on that set, use +USING+ +SCAN+.",
      queryText = "match (m:German) using scan m:German where m.surname = 'Plantikow' return m",
      assertions = (p) => assert(p.toList === List(Map("m" -> node("Stefan"))))
    )
  }
}
