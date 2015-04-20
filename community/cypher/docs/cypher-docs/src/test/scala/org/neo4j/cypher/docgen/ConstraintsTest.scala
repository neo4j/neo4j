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
import collection.JavaConverters._
import org.neo4j.kernel.api.constraints.UniquenessConstraint
import org.neo4j.cypher.CypherExecutionException

class ConstraintsTest extends DocumentingTestBase {

  def section: String = "Constraints"

  @Test def create_unique_constraint() {
    testQuery(
      title = "Create uniqueness constraint",
      text = "To create a constraint that makes sure that your database will never contain more than one node with a specific " +
        "label and one property value, use the +IS+ +UNIQUE+ syntax.",
      queryText = "CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE",
      optionalResultExplanation = "",
      assertions = (p) => assertConstraintExist("Book", "isbn")
    )
  }

  @Test def drop_unique_constraint() {
    generateConsole = false
    engine.execute("CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE")

    testQuery(
      title = "Drop uniqueness constraint",
      text = "By using +DROP+ +CONSTRAINT+, you remove a constraint from the database.",
      queryText = "DROP CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE",
      optionalResultExplanation = "",
      assertions = (p) => assertConstraintDoesNotExist("Book", "isbn")
    )
  }

  @Test def play_nice_with_constraint() {
    generateConsole = false
    engine.execute("CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE")

    testQuery(
      title = "Create a node that complies with constraints",
      text = "Create a `Book` node with an `isbn` that isn't already in the database.",
      queryText = "CREATE (book:Book {isbn: '1449356265', title: 'Graph Databases'})",
      optionalResultExplanation = "",
      assertions = (p) => assertConstraintExist("Book", "isbn")
    )
  }

  @Test def break_constraint() {
    generateConsole = false
    engine.execute("CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE")
    engine.execute("CREATE (book:Book {isbn: '1449356265', title: 'Graph Databases'})")

    testFailingQuery[CypherExecutionException](
      title = "Create a node that breaks a constraint",
      text = "Create a `Book` node with an `isbn` that is already used in the database.",
      queryText = "CREATE (book:Book {isbn: '1449356265', title: 'Graph Databases'})",
      optionalResultExplanation = "In this case the node isn't created in the graph."
    )
  }

  @Test def fail_to_create_constraint() {
    generateConsole = false
    engine.execute("CREATE (book:Book {isbn: '1449356265', title: 'Graph Databases'})")
    engine.execute("CREATE (book:Book {isbn: '1449356265', title: 'Graph Databases 2'})")

    testFailingQuery[CypherExecutionException](
      title = "Failure to create a constraint due to conflicting nodes",
      text = "Create a constraint on the property `isbn` on nodes with the `Book` label when there are two nodes with" +
        " the same `isbn`.",
      queryText = "CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE",
      optionalResultExplanation = "In this case the constraint can't be created because it is violated by existing " +
        "data. We may choose to use <<query-schema-index>> instead or remove the offending nodes and then re-apply the " +
        "constraint."
    )
  }
  
  private def assertConstraintDoesNotExist(labelName: String, propName: String) {
    assert(getConstraintIterator(labelName, propName).isEmpty, "Expected constraint iterator to be empty")
  }

  private def assertConstraintExist(labelName: String, propName: String) {
    assert(getConstraintIterator(labelName, propName).size === 1)
  }

  private def getConstraintIterator(labelName: String, propName: String): Iterator[UniquenessConstraint] = {
    val statement = db.statement

    val prop = statement.readOperations().propertyKeyGetForName(propName)
    val label = statement.readOperations().labelGetForName(labelName)

    statement.readOperations().constraintsGetForLabelAndPropertyKey(label, prop).asScala
  }
}
