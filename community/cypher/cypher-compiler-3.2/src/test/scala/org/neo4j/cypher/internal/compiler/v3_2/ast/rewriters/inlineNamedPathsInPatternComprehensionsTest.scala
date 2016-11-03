/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_2.SemanticDirection
import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite

class inlineNamedPathsInPatternComprehensionsTest extends CypherFunSuite with AstConstructionTestSupport {

  // [ ()-->() | 'foo' ]
  test("does not touch comprehensions without named path") {
    val input: ASTNode = PatternComprehension(None, RelationshipsPattern(RelationshipChain(NodePattern(None, Seq.empty, None) _, RelationshipPattern(None, false, Seq.empty, None, None, SemanticDirection.OUTGOING) _, NodePattern(None, Seq.empty, None) _) _)_, None, StringLiteral("foo")_)_

    inlineNamedPathsInPatternComprehensions(input) should equal(input)
  }

  // [ p = (a)-[r]->(b) | 'foo' ]
  test("removes named path if not used") {
    val input: PatternComprehension = PatternComprehension(Some(varFor("p")), RelationshipsPattern(RelationshipChain(NodePattern(Some(varFor("a")), Seq.empty, None) _, RelationshipPattern(Some(varFor("r")), false, Seq.empty, None, None, SemanticDirection.OUTGOING) _, NodePattern(Some(varFor("b")), Seq.empty, None) _) _)_, None, StringLiteral("foo")_)_

    inlineNamedPathsInPatternComprehensions(input) should equal(input.copy(namedPath = None)(pos))
  }

  // [ p = (a)-[r]->(b) | p ]
  test("replaces named path in projection") {
    val element: RelationshipChain = RelationshipChain(NodePattern(Some(varFor("a")), Seq.empty, None) _,
                                                       RelationshipPattern(Some(varFor("r")), false, Seq.empty, None,
                                                                           None, SemanticDirection.OUTGOING) _,
                                                       NodePattern(Some(varFor("b")), Seq.empty, None) _) _
    val input: PatternComprehension = PatternComprehension(Some(varFor("p")), RelationshipsPattern(element)_, None, Variable("p")_)_
    val output = input.copy(namedPath = None, projection = PathExpression(projectNamedPaths.patternPartPathExpression(element))_)(pos)

    inlineNamedPathsInPatternComprehensions(input) should equal(output)
  }

  // [ p = (a)-[r]->(b) WHERE p | 'foo' ]
  test("replaces named path in predicate") {
    val element: RelationshipChain = RelationshipChain(NodePattern(Some(varFor("a")), Seq.empty, None) _,
                                                       RelationshipPattern(Some(varFor("r")), false, Seq.empty, None,
                                                                           None, SemanticDirection.OUTGOING) _,
                                                       NodePattern(Some(varFor("b")), Seq.empty, None) _) _
    val input: PatternComprehension = PatternComprehension(Some(varFor("p")), RelationshipsPattern(element)_, Some(varFor("p")), StringLiteral("foo")_)_
    val output = input.copy(
      namedPath = None,
      predicate = Some(PathExpression(projectNamedPaths.patternPartPathExpression(element))_)
    )(pos)

    inlineNamedPathsInPatternComprehensions(input) should equal(output)
  }


  // [ p = (a)-[r]->(b) WHERE p | p ]
  test("replaces named path in predicate and projection") {
    val element: RelationshipChain = RelationshipChain(NodePattern(Some(varFor("a")), Seq.empty, None) _,
                                                       RelationshipPattern(Some(varFor("r")), false, Seq.empty, None,
                                                                           None, SemanticDirection.OUTGOING) _,
                                                       NodePattern(Some(varFor("b")), Seq.empty, None) _) _
    val input: PatternComprehension = PatternComprehension(Some(varFor("p")), RelationshipsPattern(element)_, Some(varFor("p")), Variable("p")_)_
    val output = input.copy(
      namedPath = None,
      predicate = Some(PathExpression(projectNamedPaths.patternPartPathExpression(element))_),
      projection = PathExpression(projectNamedPaths.patternPartPathExpression(element))_
    )(pos)

    inlineNamedPathsInPatternComprehensions(input) should equal(output)
  }
}
