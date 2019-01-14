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
package org.neo4j.cypher.internal.compiler.v3_4.ast.rewriters

import org.neo4j.cypher.internal.util.v3_4.ASTNode
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.frontend.v3_4.ast.rewriters.{inlineNamedPathsInPatternComprehensions, projectNamedPaths}
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.expressions._

class inlineNamedPathsInPatternComprehensionsTest extends CypherFunSuite with AstConstructionTestSupport {

  // [ ()-->() | 'foo' ]
  test("does not touch comprehensions without named path") {
    val input: ASTNode = PatternComprehension(None, RelationshipsPattern(RelationshipChain(NodePattern(None, Seq.empty, None) _, RelationshipPattern(None, Seq.empty, None, None, SemanticDirection.OUTGOING) _, NodePattern(None, Seq.empty, None) _) _)_, None, StringLiteral("foo")_)(pos, Set.empty)

    inlineNamedPathsInPatternComprehensions(input) should equal(input)
  }

  // [ p = (a)-[r]->(b) | 'foo' ]
  test("removes named path if not used") {
    val input: PatternComprehension = PatternComprehension(Some(varFor("p")), RelationshipsPattern(RelationshipChain(NodePattern(Some(varFor("a")), Seq.empty, None) _, RelationshipPattern(Some(varFor("r")), Seq.empty, None, None, SemanticDirection.OUTGOING) _, NodePattern(Some(varFor("b")), Seq.empty, None) _) _)_, None, StringLiteral("foo")_)(pos, Set.empty)

    inlineNamedPathsInPatternComprehensions(input) should equal(input.copy(namedPath = None)(pos, input.outerScope))
  }

  // [ p = (a)-[r]->(b) | p ]
  test("replaces named path in projection") {
    val element: RelationshipChain = RelationshipChain(NodePattern(Some(varFor("a")), Seq.empty, None) _,
                                                       RelationshipPattern(Some(varFor("r")), Seq.empty, None,
                                                                           None, SemanticDirection.OUTGOING) _,
                                                       NodePattern(Some(varFor("b")), Seq.empty, None) _) _
    val input: PatternComprehension = PatternComprehension(Some(varFor("p")), RelationshipsPattern(element)_, None, Variable("p")_)(pos, Set.empty)
    val output = input.copy(namedPath = None, projection = PathExpression(projectNamedPaths.patternPartPathExpression(element))(pos))(pos, Set.empty)

    inlineNamedPathsInPatternComprehensions(input) should equal(output)
  }

  // [ p = (a)-[r]->(b) WHERE p | 'foo' ]
  test("replaces named path in predicate") {
    val element: RelationshipChain = RelationshipChain(NodePattern(Some(varFor("a")), Seq.empty, None) _,
                                                       RelationshipPattern(Some(varFor("r")), Seq.empty, None,
                                                                           None, SemanticDirection.OUTGOING) _,
                                                       NodePattern(Some(varFor("b")), Seq.empty, None) _) _
    val input: PatternComprehension = PatternComprehension(Some(varFor("p")), RelationshipsPattern(element)_, Some(varFor("p")), StringLiteral("foo")_)(pos, Set.empty)
    val output = input.copy(
      namedPath = None,
      predicate = Some(PathExpression(projectNamedPaths.patternPartPathExpression(element))_)
    )(pos, input.outerScope)

    inlineNamedPathsInPatternComprehensions(input) should equal(output)
  }


  // [ p = (a)-[r]->(b) WHERE p | p ]
  test("replaces named path in predicate and projection") {
    val element: RelationshipChain = RelationshipChain(NodePattern(Some(varFor("a")), Seq.empty, None) _,
                                                       RelationshipPattern(Some(varFor("r")), Seq.empty, None,
                                                                           None, SemanticDirection.OUTGOING) _,
                                                       NodePattern(Some(varFor("b")), Seq.empty, None) _) _
    val input: PatternComprehension = PatternComprehension(Some(varFor("p")), RelationshipsPattern(element)_, Some(varFor("p")), Variable("p")_)(pos, Set.empty)
    val output = input.copy(
      namedPath = None,
      predicate = Some(PathExpression(projectNamedPaths.patternPartPathExpression(element))_),
      projection = PathExpression(projectNamedPaths.patternPartPathExpression(element))(pos)
    )(pos, input.outerScope)

    inlineNamedPathsInPatternComprehensions(input) should equal(output)
  }
}
