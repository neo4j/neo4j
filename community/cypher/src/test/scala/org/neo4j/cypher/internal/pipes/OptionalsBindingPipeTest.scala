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
package org.neo4j.cypher.internal.pipes

import org.scalatest.Assertions
import org.junit.Test
import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.cypher.internal.commands.values.{NotApplicable, NotBound}

class OptionalsBindingPipeTest extends Assertions {

  @Test
  def should_map_all_optionals_to_null_but_nothing_else() {
    // given
    val source = new FakePipe(List(Map("a" -> NotBound, "b" -> 12, "c" -> null, "d" -> NotApplicable)))
    val pipe = new OptionalsBindingPipe(source)

    // when
    val result = pipe.createResults(QueryStateHelper.empty).toSet

    // then
    assert( Set(ExecutionContext.from("a" -> null, "b" -> 12, "c" -> null, "d" -> null)) === result )
  }
}