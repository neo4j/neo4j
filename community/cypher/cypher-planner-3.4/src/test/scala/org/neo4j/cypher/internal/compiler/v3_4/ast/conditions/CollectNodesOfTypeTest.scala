/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_4.ast.conditions

import org.neo4j.cypher.internal.util.v3_4.ASTNode
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_4.ast.{AstConstructionTestSupport, Match}
import org.neo4j.cypher.internal.frontend.v3_4.ast.conditions.collectNodesOfType
import org.neo4j.cypher.internal.v3_4.expressions.{EveryPath, NodePattern, Pattern, Variable}

class CollectNodesOfTypeTest extends CypherFunSuite with AstConstructionTestSupport {

    private val collector: (Any => Seq[Variable]) = collectNodesOfType[Variable]()

    test("collect all variables") {
      val idA = varFor("a")
      val idB = varFor("b")
      val ast: ASTNode = Match(optional = false, Pattern(Seq(EveryPath(NodePattern(Some(idA), Seq(), Some(idB))_)))_, Seq(), None)_

      collector(ast) should equal(Seq(idA, idB))
    }

    test("collect no variable") {
      val ast: ASTNode = Match(optional = false, Pattern(Seq(EveryPath(NodePattern(None, Seq(), None)_)))_, Seq(), None)_

      collector(ast) shouldBe empty
    }
}
