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
package org.neo4j.cypher.internal.compiler.v2_0.ast

import org.neo4j.cypher.internal.compiler.v2_0._
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions

class RangeTest extends Assertions {

  @Test
  def shouldBeSingleLengthOnlyWhenUpperAndLowerAre1() {
    val token = DummyToken(0, 5)
    assertTrue(Range(Some(UnsignedInteger(1, DummyToken(0, 2))), Some(UnsignedInteger(1, DummyToken(4, 5))), token).isSingleLength)
    assertFalse(Range(None, Some(UnsignedInteger(1, DummyToken(4, 5))), token).isSingleLength)
    assertFalse(Range(Some(UnsignedInteger(1, DummyToken(0, 2))), None, token).isSingleLength)
    assertFalse(Range(Some(UnsignedInteger(1, DummyToken(0, 2))), Some(UnsignedInteger(2, DummyToken(4, 5))), token).isSingleLength)
  }
}
