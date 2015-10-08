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
package org.neo4j.cypher.docgen.cookbook

import org.neo4j.cypher.QueryStatisticsTestSupport
import org.neo4j.cypher.docgen.tooling.{DocBuilder, DocumentingTest, ResultAssertions}

class NewPrettyGraphsTest extends DocumentingTest with QueryStatisticsTestSupport {
  override def doc = new DocBuilder {
    doc("Pretty graphs", "cypher-cookbook-pretty-graphs")
    abstr("This section is showing how to create some of the http://en.wikipedia.org/wiki/Gallery_of_named_graphs[named pretty graphs on Wikipedia].")
    section("Star Graph") {
      p("The graph is created by first creating a center node, and then once per element in the range, creates a leaf node and connects it to the center.")
      query( """CREATE (center)
               |FOREACH (x IN range(1,6)| CREATE (leaf),(center)-[:X]->(leaf))
               |RETURN id(center) AS id""", assertAStarIsBorn) {
        p("The query returns the id of the center node.")
        graphViz()
      }
    }
  }.build()

  private def assertAStarIsBorn = ResultAssertions { p =>
    assertStats(p, nodesCreated = 7, relationshipsCreated = 6)
//    p.toList should equal(List(Map("id" -> 0)))
  }
}
