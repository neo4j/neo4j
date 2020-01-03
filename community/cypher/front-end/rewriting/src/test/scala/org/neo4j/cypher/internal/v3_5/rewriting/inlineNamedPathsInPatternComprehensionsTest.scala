/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.v3_5.rewriting

import org.neo4j.cypher.internal.v3_5.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.v3_5.expressions._
import org.neo4j.cypher.internal.v3_5.rewriting.rewriters.{inlineNamedPathsInPatternComprehensions, projectNamedPaths}
import org.neo4j.cypher.internal.v3_5.util.ASTNode
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_5.expressions._

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
    val output = input.copy(namedPath = None, projection = PathExpression(projectNamedPaths.patternPartPathExpression(element))(pos))(pos, input.outerScope)

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
