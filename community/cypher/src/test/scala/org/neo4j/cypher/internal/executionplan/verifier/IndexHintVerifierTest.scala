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
package org.neo4j.cypher.internal.executionplan.verifier

import org.scalatest.Assertions
import org.junit.Test

import org.neo4j.cypher.internal.executionplan.verifiers.IndexHintVerifier
import org.neo4j.cypher.internal.commands.{SchemaIndex, Equals, Or, Query}
import org.neo4j.cypher.internal.commands.expressions.{Literal, Identifier, Property}
import org.neo4j.cypher.IndexHintException

class IndexHintVerifierTest extends Assertions {

  val verifier = IndexHintVerifier

  @Test
  def throws_when_the_predicate_is_not_usable_for_index_seek() {
    //GIVEN
    val q = Query.empty.copy(
      where = Or(
        Equals(Property(Identifier("n"), "name"), Literal("Stefan")),
        Equals(Property(Identifier("n"), "age"), Literal(35))),
      hints = Seq(SchemaIndex("n", "Person", "name", None)))

    //THEN
    intercept[IndexHintException](verifier.verify(q))
  }
}