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

class NoReferenceEqualityAmongIdentifiersTest extends CypherFunSuite with AstConstructionTestSupport {

  private val collector: (Any => Seq[String]) = noReferenceEqualityAmongIdentifiers

  test("unhappy when same identifier instance is used multiple times") {
    val id = ident("a")
    val ast: ASTNode = Match(optional = false, Pattern(Seq(EveryPath(NodePattern(Some(id), Seq(), Some(id), naked = true)_)))_, Seq(), None)_

    collector(ast) should equal(Seq(s"The instance $id is used 2 times"))
  }

  test("happy when all identifier are no reference equal") {
    val ast: ASTNode = Match(optional = false, Pattern(Seq(EveryPath(NodePattern(Some(ident("a")), Seq(), Some(ident("a")), naked = true)_)))_, Seq(), None)_

    collector(ast) shouldBe empty
  }
}
