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
package org.neo4j.cypher.internal.symbols

import org.scalatest.junit.JUnitSuite
import org.junit.Test
import org.neo4j.cypher.{CypherTypeException, SyntaxException}

class SymbolTableTest extends JUnitSuite {
  @Test def givenSymbolTableWithIdentifierWhenAskForExistingThenReturnIdentifier() {
    val symbols = new SymbolTable(Identifier("x", AnyType()))
    symbols.assertHas(Identifier("x", AnyType()))
  }

  @Test(expected = classOf[SyntaxException]) def givenEmptySymbolTableWhenAskForNonExistingThenThrows() {
    val symbols = new SymbolTable()
    symbols.assertHas(Identifier("x", AnyType()))
  }

  @Test(expected = classOf[CypherTypeException]) def givenSymbolTableWithStringIdentifierWhenAskForIterableThenThrows() {
    val symbols = new SymbolTable(Identifier("x", StringType()))
    symbols.assertHas(Identifier("x", NumberType()))
  }

  @Test def givenSymbolTableWithIntegerIdentifierWhenAskForNumberThenReturn() {
    val symbols = new SymbolTable(Identifier("x", IntegerType()))
    symbols.assertHas(Identifier("x", NumberType()))
  }

  @Test def givenSymbolTableWithIterableOfStringWhenAskForIterableOfAnyThenReturn() {
    val symbols = new SymbolTable(Identifier("x", new CollectionType(StringType())))
    symbols.assertHas(Identifier("x", new CollectionType(AnyType())))
  }

  @Test def givenSymbolTableWithStringIdentifierWhenMergedWithNumberIdentifierThenContainsBoth() {
    val symbols = new SymbolTable(Identifier("x", StringType()))
    val newSymbol = symbols.add(Identifier("y", NumberType()))

    newSymbol.assertHas(Identifier("x", StringType()))
    newSymbol.assertHas(Identifier("y", NumberType()))
  }

  @Test(expected = classOf[SyntaxException]) def shouldNotBeAbleToCreateASymbolTableWithClashingNames() {
    new SymbolTable(Identifier("x", StringType()), Identifier("x", RelationshipType()))
  }


  @Test def filteringThroughShouldWork() {
    assert(getPercolatedIdentifier(NodeType(), AnyType()) === NodeType())
    assert(getPercolatedIdentifier(AnyIterableType(), AnyType()) === AnyIterableType())
    assert(getPercolatedIdentifier(NodeType(), NodeType()) === NodeType())
  }

  private def getPercolatedIdentifier(scopeType: AnyType, symbolType: AnyType): AnyType = new SymbolTable(Identifier("x", scopeType)).actualIdentifier(Identifier("x", symbolType)).typ

}
