/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.neo4j.cypher.StatisticsChecker
import org.junit.Test
import org.neo4j.cypher.internal.helpers.GraphIcing

class SchemaIndexTest extends DocumentingTestBase with StatisticsChecker with GraphIcing {
  def graphDescription = List(
    "root X A",
    "root X B",
    "root X C",
    "A KNOWS C"
  )

  def section = "Schema Index"

  @Test def create_index_on_a_label() {
    testQuery(
      title = "Create index on a label",
      text = "To create an index on a property for all nodes that have a label, use +CREATE+ +INDEX+ +ON+. " +
        "Note that the index is not immediately available, but will be created in the background. " +
        "See the <<graphdb-neo4j-schema-indexes, indexes>> section for details.",
      queryText = "create index on :Person(name)",
      returns = "",
      assertions = (p) => assertIndexesOnLabels("Person", List(List("name")))
    )
  }


  @Test def drop_index_on_a_label() {
    prepareAndTestQuery(
      title = "Drop index on a label",
      text = "To drop an index on all nodes that have a label, use the +DROP+ +INDEX+ clause.",
      prepare = { () =>
        executeQuery("create index on :Person(name)")
      },
      queryText = "drop index on :Person(name)",
      returns = "",
      assertions = (p) => assertIndexesOnLabels("Person", List())
    )
  }

  def assertIndexesOnLabels(label: String, expectedIndexes: List[List[String]]) {
    assert(expectedIndexes === db.indexPropsForLabel(label))
  }

}
