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
package org.neo4j.cypher.internal.compiler.v1_9.commands.expressions

import org.scalatest.Assertions
import org.junit.Test
import org.neo4j.cypher.internal.compiler.v1_9.symbols.{StringType, CollectionType, SymbolTable}

class ExtractFunctionTest extends Assertions {
  @Test def apa() {
    //GIVEN
    val collection = Literal(List(1, 2, 3))
    val func = new ExtractFunction(collection, "x", StrFunction(Identifier("x")))
    val symbols = SymbolTable()

    //WHEN
    val typ = func.evaluateType(new CollectionType(StringType()), symbols)

    //THEN
    assert(typ === new CollectionType(StringType()))
  }
}
