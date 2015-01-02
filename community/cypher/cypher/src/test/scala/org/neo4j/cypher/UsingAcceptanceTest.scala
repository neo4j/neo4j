/**
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
package org.neo4j.cypher

import org.junit.Test
import org.scalatest.Assertions

class UsingAcceptanceTest extends ExecutionEngineJUnitSuite with Assertions {

  @Test
  def failIfUsingIndexWithStartClause() {
    // GIVEN
    graph.createIndex("Person", "name")

    // WHEN & THEN
    intercept[SyntaxException](
      execute("start n=node(*) using index n:Person(name) where n:Person and n.name = \"kabam\" return n"))
  }

  @Test
  def failIfUsingAnIdentifierWithLabelNotUsedInMatch() {
    // GIVEN
    graph.createIndex("Person", "name")

    // WHEN
    intercept[IndexHintException](
      execute("match n-->() using index n:Person(name) where n.name = \"kabam\" return n"))
  }

  @Test
  def failIfUsingAnHintForANonExistingIndex() {
    // GIVEN: NO INDEX

    // WHEN
    intercept[IndexHintException](
      execute("match (n:Person)-->() using index n:Person(name) where n.name = \"kabam\" return n"))
  }

  @Test
  def failIfUsingHintsWithUnusableEqualityPredicate() {
    // GIVEN
    graph.createIndex("Person", "name")

    // WHEN
    intercept[IndexHintException](
      execute("match (n:Person)-->() using index n:Person(name) where n.name <> \"kabam\" return n"))
  }

  @Test
  def failIfJoiningIndexHintsInEqualityPredicates() {
    // GIVEN
    graph.createIndex("Person", "name")
    graph.createIndex("Food", "name")

    // WHEN
    intercept[IndexHintException](
      execute("match (n:Person)-->(m:Food) using index n:Person(name) using index m:Food(name) where n.name = m.name return n"))
  }

  @Test
  def doesNotAcceptMultipleIndexHintsForTheSameIdentifier() {
    // GIVEN
    graph.createIndex("Entity", "source")
    graph.createIndex("Person", "first_name")
    createNode("source" -> "form1")
    createNode("first_name" -> "John")

    // WHEN THEN
    val e = intercept[SyntaxException] {
      execute(
        "MATCH (n:Entity:Person) " +
          "USING INDEX n:Person(first_name) " +
          "USING INDEX n:Entity(source) " +
          "WHERE n.first_name = \"John\" AND n.source = \"form1\" " +
          "RETURN n;"
      )
    }

    assert(e.getMessage.startsWith("Multiple hints for same identifier are not supported"), "must report duplicate hint")
  }

  @Test
  def doesNotAcceptMultipleScanHintsForTheSameIdentifier() {
    val e = intercept[SyntaxException] {
      execute(
        "MATCH (n:Entity:Person) " +
          "USING SCAN n:Person " +
          "USING SCAN n:Entity " +
          "WHERE n.first_name = \"John\" AND n.source = \"form1\" " +
          "RETURN n;"
      )
    }

    assert(e.getMessage.startsWith("Multiple hints for same identifier are not supported"), "must report duplicate hint")
  }

  @Test
  def doesNotAcceptMultipleMixedHintsForTheSameIdentifier() {
    val e = intercept[SyntaxException] {
      execute(
        "MATCH (n:Entity:Person) " +
          "USING SCAN n:Person " +
          "USING INDEX n:Entity(first_name) " +
          "WHERE n.first_name = \"John\" AND n.source = \"form1\" " +
          "RETURN n;"
      )
    }

    assert(e.getMessage.startsWith("Multiple hints for same identifier are not supported"), "must report duplicate hint")
  }
}
