/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compiler.v3_5.ast.conditions

import org.opencypher.v9_0.util.ASTNode
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite
import org.opencypher.v9_0.ast._
import org.opencypher.v9_0.ast.conditions.containsNoNodesOfType
import org.opencypher.v9_0.expressions.{EveryPath, NodePattern, Pattern, Variable}

class ContainsNoNodesOfTypeTest extends CypherFunSuite with AstConstructionTestSupport {

  val condition: (Any => Seq[String]) = containsNoNodesOfType[UnaliasedReturnItem]()

  test("Happy when not finding UnaliasedReturnItem") {
    val ast: ASTNode = Match(optional = false, Pattern(Seq(EveryPath(NodePattern(None, Seq(), None)_)))_, Seq(), None)_

    condition(ast) should equal(Seq())
  }

  test("Fails when finding UnaliasedReturnItem") {
    val ast: ASTNode = Return(false, ReturnItems(includeExisting = false, Seq(UnaliasedReturnItem(Variable("foo")_, "foo")_))_, None, None, None, None)_

    condition(ast) should equal(Seq("Expected none but found UnaliasedReturnItem at position line 1, column 0 (offset: 0)"))
  }
}
