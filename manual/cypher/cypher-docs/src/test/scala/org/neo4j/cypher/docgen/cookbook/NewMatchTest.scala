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

import org.neo4j.cypher.docgen.tooling.ImageType.INITIAL
import org.neo4j.cypher.docgen.tooling._
import org.neo4j.graphdb.Node
import org.neo4j.tooling.GlobalGraphOperations
import org.scalatest.{Assertions, FunSuiteLike, Matchers}

import scala.collection.JavaConverters._

class NewMatchTest extends NewDocumentingTestBase {
  val doc =
    Document("Match",
      initQueries = Seq("FUNKY CYPHER THAT BUILDS DATABASE"),
      Abstract("The `MATCH` clause is used to search for the pattern described in it.") ~
        Section("Introduction",
          Paragraph(
            s"""The `MATCH` clause allows you to specify the patterns Neo4j will search for in the database.
               |This is the primary way of getting data into the current set of bindings.
               |It is worth reading up more on the specification of the patterns themselves in <<introduction-pattern>>""".stripMargin) ~
            Paragraph(
              """MATCH is often coupled to a WHERE part which adds restrictions, or predicates, to the MATCH patterns, making them more specific.
                |The predicates are part of the pattern description, not a filter applied after the matching is done.
                |This means that WHERE should always be put together with the MATCH clause it belongs to.""".stripMargin) ~
            Tip("To understand more about the patterns used in the MATCH clause, read Section 9.6, “Patterns”") ~
            Paragraph("The following graph is used for the examples below:") ~
            GraphImage(INITIAL)
        ) ~
        Section("Basic node finding",
          Section("Get all nodes",
            Paragraph("By just specifying a pattern with a single node and no labels, all nodes in the graph will be returned.") ~
              Query("MATCH (n) RETURN n",
                assertions = ResultAndDbAssertions((p, db) => {
                  val allNodes: List[Node] = GlobalGraphOperations.at(db).getAllNodes.asScala.toList
                  allNodes should equal(p.columnAs[Node]("n").toList)
                }),

                Paragraph("Returns all the nodes in the database.") ~
                  QueryResultTable
              )
          )
        )
    )
}




trait NewDocumentingTestBase extends FunSuiteLike with Assertions with Matchers {
  def doc: Document
}

