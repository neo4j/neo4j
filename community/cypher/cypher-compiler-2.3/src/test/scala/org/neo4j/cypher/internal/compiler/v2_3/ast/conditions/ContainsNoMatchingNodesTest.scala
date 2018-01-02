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
package org.neo4j.cypher.internal.compiler.v2_3.ast.conditions

import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class ContainsNoMatchingNodesTest extends CypherFunSuite with AstConstructionTestSupport {

  val condition: (Any => Seq[String]) = containsNoMatchingNodes({
    case ri: ReturnItems if ri.includeExisting => "ReturnItems(includeExisting = true, ...)"
  })

  test("Happy when not finding ReturnItems(includeExisting = true, ...)") {
    val ast: ASTNode = Return(false, ReturnItems(includeExisting = false, Seq(UnaliasedReturnItem(Identifier("foo")_, "foo")_))_, None, None, None)_

    condition(ast) should equal(Seq())
  }

  test("Fails when finding ReturnItems(includeExisting = true, ...)") {
    val ast: ASTNode = Return(false, ReturnItems(includeExisting = true, Seq(UnaliasedReturnItem(Identifier("foo")_, "foo")_))_, None, None, None)_

    condition(ast) should equal(Seq("Expected none but found ReturnItems(includeExisting = true, ...) at position line 1, column 0 (offset: 0)"))
  }
}
