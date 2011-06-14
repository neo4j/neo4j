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

import org.junit.Test
import commands.{NodeIdentifier, RelationshipIdentifier}
import org.junit.Assert._


class SymbolTableTest {
  @Test def testConcatenating() {
    val table1 = new SymbolTable(Map("node"->NodeIdentifier("node")))
    val table2 = new SymbolTable(Map("rel"->RelationshipIdentifier("rel")))

    val result = table1 ++ table2

    assertEquals(Map("node" -> NodeIdentifier("node"), "rel" -> RelationshipIdentifier("rel")), result.identifiers)
  }


  @Test(expected = classOf[SyntaxError]) def shouldNotOverwriteSymbolsWithNewType() {
    val table1 = new SymbolTable(Map("x"->NodeIdentifier("x")))
    val table2 = new SymbolTable(Map("x"->RelationshipIdentifier("x")))

    table1 ++ table2
  }

  @Test def registreringTwiceIsOk() {
    val table1 = new SymbolTable(Map("x"->NodeIdentifier("x")))
    val table2 = new SymbolTable(Map("x"->NodeIdentifier("x")))

    val result = table1 ++ table2

    assertEquals(Map("x" -> NodeIdentifier("x")), result.identifiers)
  }
}