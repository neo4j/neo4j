/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.ast.rewriters

import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class ReplaceLiteralDynamicPropertyLookupsTest extends CypherFunSuite with AstConstructionTestSupport {

  test("Replaces literal dynamic property lookups") {
    val input: ASTNode = ContainerIndex(ident("a"), StringLiteral("name")_)_
    val output: ASTNode = Property(ident("a"), PropertyKeyName("name")_)_

    replaceLiteralDynamicPropertyLookups(input) should equal(output)
  }

  test("Does not replaces non-literal dynamic property lookups") {
    val input: ASTNode = ContainerIndex(ident("a"), ident("b"))_

    replaceLiteralDynamicPropertyLookups(input) should equal(input)
  }
}
