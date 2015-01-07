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
package org.neo4j.cypher.internal.compiler.v1_9.mutation

import org.scalatest.Assertions
import org.junit.Test
import org.neo4j.cypher.CypherTypeException

class PropertySetActionTest extends Assertions {

  val x = new Object with GraphElementPropertyFunctions

  @Test def string_collection_turns_into_string_array() {
    assert(x.makeValueNeoSafe(Seq("a", "b")) === Array("a", "b"))
  }

  @Test def empty_collection_in_is_empty_array() {
    assert(x.makeValueNeoSafe(Seq()) === Array())
  }

  @Test def mixed_types_are_not_ok() {
    intercept[CypherTypeException](x.makeValueNeoSafe(Seq("a", 12, false)))
  }
}
