/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import commands.{VarLengthRelatedTo, RelatedTo}
import org.junit.Test
import org.scalatest.{Spec, Assertions}
import org.neo4j.graphdb.Direction

class SymbolsExtractionTest extends Spec with Assertions
{
  val engine = new ExecutionEngine(null)

  @Test def singleRelateTo()
  {
    val r = RelatedTo("a", "b", "r", None, Direction.BOTH, false)

    assert(engine.extractSymbols(Seq(r)) === Set("a", "b", "r"))
  }

  @Test def singleVarLengthRelatedTo()
  {
    val r = VarLengthRelatedTo("p", "a", "b", None, None, None, Direction.BOTH, false)

    assert(engine.extractSymbols(Seq(r)) === Set("p", "a", "b"))
  }

  @Test def altogetherNow()
  {
    val r1 = VarLengthRelatedTo("p", "x", "y", None, None, None, Direction.BOTH, false)
    val r2 = RelatedTo("a", "b", "r", None, Direction.BOTH, false)

    assert(engine.extractSymbols(Seq(r1, r2)) === Set("p", "x", "y", "a", "b", "r"))
  }

  @Test def overlapsAreRemoved()
  {
    val r1 = VarLengthRelatedTo("p", "a", "b", None, None, None, Direction.BOTH, false)
    val r2 = RelatedTo("a", "b", "r", None, Direction.BOTH, false)

    assert(engine.extractSymbols(Seq(r1, r2)) === Set("p", "a", "b", "r"))
  }
}


