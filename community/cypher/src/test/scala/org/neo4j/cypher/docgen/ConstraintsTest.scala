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

import org.junit.Test
import collection.JavaConverters._
import org.neo4j.kernel.api.constraints.UniquenessConstraint

class ConstraintsTest extends DocumentingTestBase {
  def graphDescription: List[String] = List()

  def section: String = "Constraints"

  @Test def create_unique_constraint() {
    testQuery(
      title = "Create uniqueness constraint",
      text = "To create a constraint that makes sure that your database will never contain more than one node with a specific" +
        "label and one property value, use the +IS+ +UNIQUE+ syntax.",
      queryText = "CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE",
      returns = "",
      assertions = (p) => assertConstraintExist("Book", "isbn")
    )
  }

  @Test def drop_unique_constraint() {
    engine.execute("CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE")

    testQuery(
      title = "Drop uniqueness constraint",
      text = "By using +DROP+ +CONSTRAINT+, you remove a constraint from the database.",
      queryText = "DROP CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE",
      returns = "",
      assertions = (p) => assertConstraintDoesNotExist("Book", "isbn")
    )
  }

  private def assertConstraintDoesNotExist(labelName: String, propName: String) {
    assert(getConstraintIterator(labelName, propName).isEmpty, "Expected constraint iterator to be empty")
  }

  private def assertConstraintExist(labelName: String, propName: String) {
    assert(getConstraintIterator(labelName, propName).size === 1)
  }

  private def getConstraintIterator(labelName: String, propName: String): Iterator[UniquenessConstraint] = {
    val statementCtx = db.statementContextForReading

    val prop = statementCtx.propertyKeyGetForName(propName)
    val label = statementCtx.labelGetForName(labelName)

    statementCtx.constraintsGetForLabelAndPropertyKey(label, prop).asScala
  }
}