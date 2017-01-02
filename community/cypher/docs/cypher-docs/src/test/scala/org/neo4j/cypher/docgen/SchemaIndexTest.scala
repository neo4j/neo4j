/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.cypher.QueryStatisticsTestSupport
import org.junit.Assert._
import org.junit.Test
import org.neo4j.cypher.internal.helpers.GraphIcing
import org.hamcrest.CoreMatchers._

import scala.collection.immutable.IndexedSeq

class SchemaIndexTest extends DocumentingTestBase with QueryStatisticsTestSupport with GraphIcing {

  //need a couple of 'Person' to make index operations more efficient than label scans
  override val setupQueries = (1 to 20 map (_ => """CREATE (:Person)""")).toList

  override def graphDescription = List(
    "andres:Person KNOWS mark:Person"
  )

  override val properties = Map(
    "andres" -> Map("name" -> "Andres"),
    "mark" -> Map("name" -> "Mark")
  )

  override val setupConstraintQueries = List(
    "CREATE INDEX ON :Person(name)"
  )

  def section = "Schema Index"

  @Test def create_index_on_a_label() {
    testQuery(
      title = "Create index on a label",
      text = "To create an index on a property for all nodes that have a label, use +CREATE+ +INDEX+ +ON+. " +
        "Note that the index is not immediately available, but will be created in the background. " +
        "See <<graphdb-neo4j-schema-indexes>> for details.",
      queryText = "create index on :Person(name)",
      optionalResultExplanation = "",
      assertions = (p) => assertIndexesOnLabels("Person", List(List("name")))
    )
  }

  @Test def drop_index_on_a_label() {
    prepareAndTestQuery(
      title = "Drop index on a label",
      text = "To drop an index on all nodes that have a label, use the +DROP+ +INDEX+ clause.",
      prepare = executePreparationQueries(List("create index on :Person(name)")),
      queryText = "drop index on :Person(name)",
      optionalResultExplanation = "",
      assertion = (p) => assertIndexesOnLabels("Person", List())
    )
  }

  @Test def use_index() {
    testQuery(
      title = "Use index",
      text = "There is usually no need to specify which indexes to use in a query, Cypher will figure that out by itself. " +
        "For example the query below will use the `Person(name)` index, if it exists. " +
        "If you for some reason want to hint to specific indexes, see <<query-using>>.",
      queryText = "match (person:Person {name: 'Andres'}) return person",
      optionalResultExplanation = "",
      assertions = {
        (p) =>
          assertEquals(1, p.size)
          assertThat(p.executionPlanDescription().toString, containsString("NodeIndexSeek"))
      }
    )
  }
  @Test def use_index_with_where() {
    testQuery(
      title = "Use index with WHERE",
      text = "Indexes are also automatically used for equality comparisons of a indexed property in the WHERE clause." +
        "If you for some reason want to hint to specific indexes, see <<query-using>>.",
      queryText = "match (person:Person) WHERE person.name = 'Andres' return person",
      optionalResultExplanation = "",
      assertions = {
        (p) =>
          assertEquals(1, p.size)
          assertThat(p.executionPlanDescription().toString, containsString("NodeIndexSeek"))
      }
    )
  }

  @Test def use_index_with_in() {
    testQuery(
      title = "Use index with IN",
      text =
        "The IN predicate on `person.name` in the following query will use the `Person(name)` index, if it exists. " +
        "If you for some reason want Cypher to use specific indexes, you can enforce it using hints. See <<query-using>>.",
      queryText = "match (person:Person) WHERE person.name IN ['Andres','Mark'] return person",
      optionalResultExplanation = "",
      assertions = {
        (p) =>
          assertEquals(2, p.size)
          assertThat(p.executionPlanDescription().toString, containsString("NodeIndexSeek"))
      }
    )
  }

  def assertIndexesOnLabels(label: String, expectedIndexes: List[List[String]]) {
    assert(expectedIndexes === db.indexPropsForLabel(label))
  }

}
