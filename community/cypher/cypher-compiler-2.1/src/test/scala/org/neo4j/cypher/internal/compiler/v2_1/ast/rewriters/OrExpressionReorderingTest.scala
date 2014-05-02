/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.ast.rewriters

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.bottomUp
import org.neo4j.graphdb.Direction

class OrExpressionReorderingTest extends CypherFunSuite with AstConstructionTestSupport {

  private val rewriter = bottomUp(orExpressionReordering)

  private val exp1 = True()_
  private val exp2 = Not(False()_)_

  private val nodePattern: NodePattern = NodePattern(None,Seq.empty,None,false)_
  private val relPattern: RelationshipPattern = RelationshipPattern(None,false,Seq.empty,None,None,Direction.BOTH)_
  private val patternExp1 = PatternExpression(RelationshipsPattern(RelationshipChain(nodePattern, relPattern, nodePattern)_)_)
  private val patternExp2 = PatternExpression(RelationshipsPattern(RelationshipChain(nodePattern, relPattern, nodePattern)_)_)

  test("should do nothing if there are no pattern expressions in the ors") {
    val input: Ors = Ors(List(exp1, exp2))_

    val result = rewriter(input).get

    result should equal(input)
  }

  test("should do nothing if there are only pattern expressions in the ors") {
    val input: Ors = Ors(List(patternExp1, patternExp2))_

    val result = rewriter(input).get

    result should equal(input)
  }

  test("should move up front the pattern expressions") {
    val input: Ors = Ors(List(exp1, patternExp1, exp2, patternExp2))_

    val result = rewriter(input).get

    val expected: Ors = Ors(List(patternExp1, patternExp2, exp1, exp2))_
    result should equal(expected)
  }
}
