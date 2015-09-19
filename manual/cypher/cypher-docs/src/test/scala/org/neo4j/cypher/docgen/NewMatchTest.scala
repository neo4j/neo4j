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

import org.neo4j.cypher.docgen.tooling._
import org.neo4j.graphdb.Node
import org.neo4j.tooling.GlobalGraphOperations
import collection.JavaConverters._

class NewMatchTest extends NewDocumentingTestBase {
  override def doc = new DocBuilder {
    doc("Match", "query-match")
    initQueries(
      "CREATE (:Person {name:'Apa'})"
    )
    abstraCt("The `MATCH` clause is used to search for the pattern described in it.")
    section("Introduction") {
      p(
        """The `MATCH` clause allows you to specify the patterns Neo4j will search for in the database.
          |This is the primary way of getting data into the current set of bindings.
          |It is worth reading up more on the specification of the patterns themselves in <<introduction-pattern>>""")
      p(
        """MATCH is often coupled to a WHERE part which adds restrictions, or predicates, to the MATCH patterns, making them more specific.
          |The predicates are part of the pattern description, not a filter applied after the matching is done.
          |This means that WHERE should always be put together with the MATCH clause it belongs to.""")
      tip {
        p("To understand more about the patterns used in the MATCH clause, read <<query-pattern>>")
      }
    }
    section("Basic node finding") {
      section("Get all nodes") {
        p("By just specifying a pattern with a single node and no labels, all nodes in the graph will be returned.")
        query("MATCH (n) RETURN n", assert1) {
          p("Returns all the nodes in the database.")
          resultTable()
        }
      }
    }
  }.build()

  private def assert1 = ResultAndDbAssertions((p, db) => {
    val tx = db.beginTx()
    try {
      val allNodes: List[Node] = GlobalGraphOperations.at(db).getAllNodes.asScala.toList
      allNodes should equal(p.columnAs[Node]("n").toList)
    } finally tx.close()
  })
}
