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
package org.neo4j.cypher.internal.compiler.v3_3.ast.conditions

import org.neo4j.cypher.internal.frontend.v3_3.SemanticDirection
import org.neo4j.cypher.internal.frontend.v3_3.ast.conditions.noUnnamedPatternElementsInPatternComprehension
import org.neo4j.cypher.internal.frontend.v3_3.ast.{RelationshipPattern, StringLiteral, _}
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite

class noUnnamedPatternElementsInPatternComprehensionTest extends CypherFunSuite with AstConstructionTestSupport {

  private val condition: (Any => Seq[String]) = noUnnamedPatternElementsInPatternComprehension

  test("should detect an unnamed pattern element in comprehension") {
    val input: ASTNode = PatternComprehension(None, RelationshipsPattern(
      RelationshipChain(NodePattern(None, Seq.empty, None) _,
                        RelationshipPattern(None, Seq.empty, None, None, SemanticDirection.OUTGOING) _,
                        NodePattern(None, Seq.empty, None) _) _) _, None, StringLiteral("foo") _) _

    condition(input) should equal(Seq(s"Expression $input contains pattern elements which are not named"))
  }

  test("should not react to fully named pattern comprehension") {
    val input: PatternComprehension = PatternComprehension(Some(varFor("p")),
                                                           RelationshipsPattern(RelationshipChain(NodePattern(Some(varFor("a")), Seq.empty, None) _,
                                                                                                  RelationshipPattern(Some(varFor("r")), Seq.empty, None, None, SemanticDirection.OUTGOING) _,
                                                                                                  NodePattern(Some(varFor("b")), Seq.empty, None) _) _) _,
                                                           None,
                                                           StringLiteral("foo")_)_

    condition(input) shouldBe empty
  }
}
