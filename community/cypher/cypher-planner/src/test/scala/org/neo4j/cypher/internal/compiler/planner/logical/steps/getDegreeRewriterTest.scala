/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions.GetDegree
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.RelationshipsPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.functions.Exists
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class getDegreeRewriterTest extends CypherFunSuite with AstConstructionTestSupport {

  test("Rewrite exists( (a)-[:FOO]->() ) to GetDegree( (a)-[:FOO]->() ) > 0") {
    val incoming = Exists.asInvocation(
      PatternExpression(RelationshipsPattern(RelationshipChain(NodePattern(Some(varFor("a")), Seq.empty, None)(pos),
        RelationshipPattern(None, Seq(RelTypeName("FOO")(pos)), None, None, SemanticDirection.OUTGOING)(pos),
        NodePattern(None, Seq.empty, None)(pos))(pos))(pos)))(pos)
    val expected = greaterThan(GetDegree(varFor("a"), Some(RelTypeName("FOO")(pos)), SemanticDirection.OUTGOING)(pos), literalInt(0))

    getDegreeRewriter(incoming) should equal(expected)
  }

  test("Rewrite exists( ()-[:FOO]->(a) ) to GetDegree( (a)<-[:FOO]-() ) > 0") {
    val incoming = Exists.asInvocation(
      PatternExpression(RelationshipsPattern(RelationshipChain(NodePattern(None, Seq.empty, None)(pos),
        RelationshipPattern(None, Seq(RelTypeName("FOO")(pos)), None, None, SemanticDirection.OUTGOING)(pos),
        NodePattern(Some(varFor("a")), Seq.empty, None)(pos))(pos))(pos)))(pos)
    val expected = greaterThan(GetDegree(varFor("a"), Some(RelTypeName("FOO")(pos)), SemanticDirection.INCOMING)(pos), literalInt(0))

    getDegreeRewriter(incoming) should equal(expected)
  }
}
