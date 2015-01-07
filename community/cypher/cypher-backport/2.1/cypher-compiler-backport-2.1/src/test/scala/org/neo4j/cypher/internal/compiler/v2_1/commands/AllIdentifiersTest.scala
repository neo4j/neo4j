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
package org.neo4j.cypher.internal.compiler.v2_1.commands

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.commands.expressions.Identifier
import org.neo4j.cypher.internal.compiler.v2_1.symbols._

class AllIdentifiersTest extends CypherFunSuite {
  val x = AllIdentifiers()

  test("nodes") {
    val symbols = getSymbols("n" -> CTNode)

     x.expressions(symbols) should equal(Map("n" -> Identifier("n")))
  }

  test("relationships") {
    val symbols = getSymbols("r" -> CTRelationship)

     x.expressions(symbols) should equal(Map("r" -> Identifier("r")))
  }

  test("paths") {
    val symbols = getSymbols("p" -> CTPath)

     x.expressions(symbols) should equal(Map("p" -> Identifier("p")))
  }

  private def getSymbols(k: (String, CypherType)*): SymbolTable =
    SymbolTable(k.toMap)
}
