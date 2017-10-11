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
package org.neo4j.cypher.internal.frontend.v3_2.ast

import org.neo4j.cypher.internal.frontend.v3_2.symbols._
import org.neo4j.cypher.internal.frontend.v3_2.{SemanticDirection, SemanticState}
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite

class ShortestPathExpressionTest extends CypherFunSuite with AstConstructionTestSupport {

  test("should get correct types for shortestPath") {
    // Given
    val (exp, state) = makeShortestPathExpression(true)

    // When
    val result = exp.semanticCheck(Expression.SemanticContext.Simple)(state)

    // Then
    result.errors shouldBe empty
    exp.types(result.state) should equal(TypeSpec.exact(CTPath))
  }

  test("should get correct types for allShortestPath") {
    // Given
    val (exp, state) = makeShortestPathExpression(false)

    // When
    val result = exp.semanticCheck(Expression.SemanticContext.Simple)(state)

    // Then
    result.errors shouldBe empty
    exp.types(result.state) should equal(TypeSpec.exact(CTList(CTPath)))
  }

  private def makeShortestPathExpression(single: Boolean): (ShortestPathExpression, SemanticState) = {
    val state = Seq("n", "k").foldLeft(SemanticState.clean) { (acc, n) =>
      acc.specifyType(varFor(n), TypeSpec.exact(CTNode)).right.get
    }
    val pattern = chain(node(Some(varFor("n"))), relationship(None), node(Some(varFor("k"))))
    (ShortestPathExpression(ShortestPaths(pattern, single) _), state)
  }
  private def chain(left: PatternElement, rel: RelationshipPattern, right: NodePattern): RelationshipChain = {
    RelationshipChain(left, rel, right)_
  }

  private def relationship(id: Option[Variable]): RelationshipPattern = {
    RelationshipPattern(id, Seq.empty, None, None, SemanticDirection.OUTGOING)_
  }

  private def node(id: Option[Variable]): NodePattern = {
    NodePattern(id, Seq.empty, None)_
  }

}
