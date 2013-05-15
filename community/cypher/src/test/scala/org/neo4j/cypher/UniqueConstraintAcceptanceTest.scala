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

import org.neo4j.cypher.internal.helpers.CollectionSupport
import org.scalatest.Assertions
import org.junit.{Ignore, Test}
import org.junit.Assert._
import collection.JavaConverters._


class UniqueConstraintAcceptanceTest extends ExecutionEngineHelper with StatisticsChecker with Assertions with CollectionSupport {
  @Test
  def should_add_constraint() {
    //GIVEN

    //WHEN
    parseAndExecute("create constraint on (identifier:Label) assert identifier.propertyKey is unique")

    //THEN
    val statementCtx = graph.statementContextForReading

    val prop = statementCtx.getPropertyKeyId("propertyKey")
    val label = statementCtx.getLabelId("Label")

    val constraints = statementCtx.getConstraints(label, prop).asScala

    assert(constraints.size === 1)
  }
  @Test
  def should_drop_constraint() {
    //GIVEN
    parseAndExecute("create constraint on (identifier:Label) assert identifier.propertyKey is unique")

    //WHEN
    parseAndExecute("drop constraint on (identifier:Label) assert identifier.propertyKey is unique")

    //THEN
    val statementCtx = graph.statementContextForReading

    val prop = statementCtx.getPropertyKeyId("propertyKey")
    val label = statementCtx.getLabelId("Label")

    val constraints = statementCtx.getConstraints(label, prop).asScala

    assertTrue("No constraints should exist", constraints.isEmpty)
  }
  @Ignore("2013-05-15 Lucene indexes don't support verifying constraints on index population yet.")
  @Test
  def should_fail_to_add_constraint_when_existing_data_conflicts() {
    // GIVEN
    parseAndExecute("create (a:Person{id:1}), (b:Person{id:1})")

    // WHEN
    parseAndExecute("create constraint on (n:Person) assert n.id is unique")

    // THEN
    val statementCtx = graph.statementContextForReading

    val prop = statementCtx.getPropertyKeyId("id")
    val label = statementCtx.getLabelId("Person")

    val constraints = statementCtx.getConstraints(label, prop).asScala

    assertTrue("No constraints should exist", constraints.isEmpty)
  }
}