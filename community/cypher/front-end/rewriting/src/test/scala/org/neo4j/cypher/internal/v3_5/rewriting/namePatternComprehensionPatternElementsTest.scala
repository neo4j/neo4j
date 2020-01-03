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
import org.neo4j.cypher.internal.v3_5.rewriting.rewriters.namePatternComprehensionPatternElements
import org.neo4j.cypher.internal.v3_5.util.ASTNode
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_5.expressions.{NodePattern, RelationshipChain, RelationshipPattern, RelationshipsPattern}

class namePatternComprehensionPatternElementsTest extends CypherFunSuite with AstConstructionTestSupport {

  test("should name all pattern elements in a comprehension") {
    val input: ASTNode = PatternComprehension(None, RelationshipsPattern(
      RelationshipChain(NodePattern(None, Seq.empty, None) _,
                        RelationshipPattern(None, Seq.empty, None, None, SemanticDirection.OUTGOING) _,
                        NodePattern(None, Seq.empty, None) _) _) _, None, StringLiteral("foo") _)(pos, Set.empty)

    namePatternComprehensionPatternElements(input) match {
      case PatternComprehension(_, RelationshipsPattern(RelationshipChain(NodePattern(Some(_), _, _, _),
                                                                          RelationshipPattern(Some(_), _, _, _, _, _, _),
                                                                          NodePattern(Some(_), _, _, _))), _, _) => ()
      case _ => fail("All things were not named")
    }
  }

  test("should not change names of already named things") {
    val input: PatternComprehension = PatternComprehension(Some(varFor("p")),
                                                           RelationshipsPattern(RelationshipChain(NodePattern(Some(varFor("a")), Seq.empty, None) _,
                                                                                                  RelationshipPattern(Some(varFor("r")), Seq.empty, None, None, SemanticDirection.OUTGOING) _,
                                                                                                  NodePattern(Some(varFor("b")), Seq.empty, None) _) _) _,
                                                           None,
                                                           StringLiteral("foo")_)(pos, Set.empty)

    namePatternComprehensionPatternElements(input) should equal(input)
  }
}
