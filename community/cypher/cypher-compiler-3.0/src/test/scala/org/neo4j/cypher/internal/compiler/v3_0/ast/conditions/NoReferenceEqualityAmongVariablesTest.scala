/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_0.ast.conditions

import org.neo4j.cypher.internal.frontend.v3_0.ast._
import org.neo4j.cypher.internal.frontend.v3_0.test_helpers.CypherFunSuite

class NoReferenceEqualityAmongVariablesTest extends CypherFunSuite with AstConstructionTestSupport {

  private val collector: (Any => Seq[String]) = noReferenceEqualityAmongVariables

  test("unhappy when same Variable instance is used multiple times") {
    val id = varFor("a")
    val ast: ASTNode = Match(optional = false, Pattern(Seq(EveryPath(NodePattern(Some(id), Seq(), Some(id))_)))_, Seq(), None)_

    collector(ast) should equal(Seq(s"The instance $id is used 2 times"))
  }

  test("happy when all variable are no reference equal") {
    val ast: ASTNode = Match(optional = false, Pattern(Seq(EveryPath(NodePattern(Some(varFor("a")), Seq(), Some(varFor("a")))_)))_, Seq(), None)_

    collector(ast) shouldBe empty
  }
}
