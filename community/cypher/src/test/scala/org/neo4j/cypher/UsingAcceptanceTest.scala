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
package org.neo4j.cypher

import internal.helpers.GraphIcing
import org.junit.Test
import org.scalatest.Assertions
import org.neo4j.cypher.internal.spi.QueryContext
import org.neo4j.cypher.internal.spi.gdsimpl.TransactionBoundQueryContext
import org.neo4j.kernel.EmbeddedGraphDatabase

class UsingAcceptanceTest extends ExecutionEngineHelper with Assertions with GraphIcing {

  @Test(expected = /* THEN */ classOf[SyntaxException])
  def failIfUsingIndexWithStartClause() {
    // GIVEN
    graph.createIndex("Person", "name")

    // WHEN
    parseAndExecute("start n=node(*) using index n:Person(name) where n:Person and n.name = \"kabam\" return n")
  }

  @Test(expected = /* THEN */ classOf[IndexHintException])
  def failIfUsingAnIdentifierWithLabelNotUsedInMatch() {
    // GIVEN
    graph.createIndex("Person", "name")

    // WHEN
    parseAndExecute("match n-->() using index n:Person(name) where n.name = \"kabam\" return n")
  }

  @Test(expected = /* THEN */ classOf[IndexHintException])
  def failIfUsingAnHintForANonExistingIndex() {
    // GIVEN: NO INDEX

    // WHEN
    parseAndExecute("match n:Person-->() using index n:Person(name) where n.name = \"kabam\" return n")
  }

  @Test(expected = /* THEN */ classOf[IndexHintException])
  def failIfUsingAnHintWithAnUnknownIdentifier() {
    // GIVEN: NO INDEX

    // WHEN
    parseAndExecute("match n:Person-->() using index m:Person(name) where n.name = \"kabam\" return n")
  }

  @Test(expected = /* THEN */ classOf[IndexHintException])
  def failIfUsingHintsWithUnusableEqualityPredicate() {
    // GIVEN
    graph.createIndex("Person", "name")

    // WHEN
    parseAndExecute("match n:Person-->() using index n:Person(name) where n.name <> \"kabam\" return n")
  }

  @Test(expected = /* THEN */ classOf[IndexHintException])
  def failIfJoiningIndexHintsInEqualityPredicates() {
    // GIVEN
    graph.createIndex("Person", "name")
    graph.createIndex("Food", "name")

    // WHEN
    parseAndExecute("match n:Person-->m:Food using index n:Person(name) using index m:Food(name) where n.name = m.name return n")
  }
}