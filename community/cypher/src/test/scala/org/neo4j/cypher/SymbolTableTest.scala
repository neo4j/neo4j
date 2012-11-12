/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import commands._
import org.junit.Test
import org.junit.Assert._
import org.scalatest.junit.JUnitSuite

class SymbolTableTest extends JUnitSuite {
  @Test def testConcatenating() {
    val table1 = new SymbolTable(NodeIdentifier("node"))
    val table2 = new SymbolTable(RelationshipIdentifier("rel"))

    val result = table1 ++ table2

    assertEquals(Set(NodeIdentifier("node"), RelationshipIdentifier("rel")), result.identifiers)
  }


  @Test(expected = classOf[SyntaxException]) def shouldNotOverwriteSymbolsWithNewType() {
    val table1 = new SymbolTable(NodeIdentifier("x"))
    val table2 = new SymbolTable(RelationshipIdentifier("x"))

    table1 ++ table2
  }

  @Test def registreringTwiceIsOk() {
    val table1 = new SymbolTable(NodeIdentifier("x"))
    val table2 = new SymbolTable(NodeIdentifier("x"))

    val result = table1 ++ table2

    assertEquals(Set(NodeIdentifier("x")), result.identifiers)
  }

  @Test def shouldResolveUnboundIdentifiers() {
    val table1 = new SymbolTable(NodeIdentifier("x"))
    val table2 = new SymbolTable(UnboundIdentifier("x", None))

    val result = table1 ++ table2

    assertEquals(Set(NodeIdentifier("x")), result.identifiers)
  }

  @Test def shouldResolveUnboundConcreteIdentifiers() {
    val table1 = new SymbolTable(NodeIdentifier("x"))
    val table2 = new SymbolTable(UnboundIdentifier("x", Some(PropertyIdentifier("x", "name"))))

    val result = table1 ++ table2

    assertEquals(Set(NodeIdentifier("x"), PropertyIdentifier("x", "name")), result.identifiers)
  }

  @Test(expected = classOf[SyntaxException]) def shouldFailForUnboundConcreteIdentifiers() {
    val table1 = new SymbolTable()
    val table2 = new SymbolTable(UnboundIdentifier("x", Some(PropertyIdentifier("x", "name"))))

    table1 ++ table2
  }

  @Test(expected = classOf[SyntaxException]) def shouldFailForUnbound() {
    val table1 = new SymbolTable()
    val table2 = new SymbolTable(UnboundIdentifier("x", None))

    table1 ++ table2
  }


  @Test(expected = classOf[SyntaxException]) def shouldFailWhenExpectingAPropertyContainer() {
    val table = new SymbolTable(LiteralIdentifier("x"))
    table.assertHas(PropertyContainerIdentifier("x"))
  }

  @Test def shouldFindAMatchingIdentifier() {
    val table = new SymbolTable(NodeIdentifier("x"))
    table.assertHas(PropertyContainerIdentifier("x"))
  }

  @Test def shouldFindTheConcreteIdentifier() {
    val table = new SymbolTable(LiteralIdentifier("x"))
    table.assertHas(LiteralIdentifier("x"))
  }
}