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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical.steps

import org.opencypher.v9_0.util.test_helpers.CypherFunSuite
import org.opencypher.v9_0.ast.AstConstructionTestSupport
import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.expressions.functions.Exists

class getDegreeRewriterTest extends CypherFunSuite with AstConstructionTestSupport {

  test("Rewrite exists( (a)-[:FOO]->() ) to GetDegree( (a)-[:FOO]->() ) > 0") {
    val incoming = Exists.asInvocation(
      PatternExpression(RelationshipsPattern(RelationshipChain(NodePattern(Some(varFor("a")), Seq.empty, None)(pos),
                                                               RelationshipPattern(None, Seq(RelTypeName("FOO")(pos)), None, None, SemanticDirection.OUTGOING)(pos),
                                                               NodePattern(None, Seq.empty, None)(pos))(pos))(pos)))(pos)
    val expected = GreaterThan(GetDegree(varFor("a"), Some(RelTypeName("FOO")(pos)), SemanticDirection.OUTGOING)(pos), SignedDecimalIntegerLiteral("0")(pos))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test("Rewrite exists( ()-[:FOO]->(a) ) to GetDegree( (a)<-[:FOO]-() ) > 0") {
    val incoming = Exists.asInvocation(
      PatternExpression(RelationshipsPattern(RelationshipChain(NodePattern(None, Seq.empty, None)(pos),
                                                               RelationshipPattern(None, Seq(RelTypeName("FOO")(pos)), None, None, SemanticDirection.OUTGOING)(pos),
                                                               NodePattern(Some(varFor("a")), Seq.empty, None)(pos))(pos))(pos)))(pos)
    val expected = GreaterThan(GetDegree(varFor("a"), Some(RelTypeName("FOO")(pos)), SemanticDirection.INCOMING)(pos), SignedDecimalIntegerLiteral("0")(pos))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }
}
