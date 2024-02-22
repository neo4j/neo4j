/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.rewriting.conditions

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.expressions.MatchMode
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class CollectNodesOfTypeTest extends CypherFunSuite with AstConstructionTestSupport {

  private val collector: Any => Seq[Variable] =
    collectNodesOfType[Variable]().apply(_)(CancellationChecker.NeverCancelled)

  test("collect all variables") {
    val idA = varFor("a")
    val idB = varFor("b")
    val nodePattern = NodePattern(Some(idA), None, Some(idB), None) _
    val ast: ASTNode =
      Match(
        optional = false,
        matchMode = MatchMode.default(pos),
        patternForMatch(nodePattern),
        Seq(),
        None
      ) _

    collector(ast) should equal(Seq(idA, idB))
  }

  test("collect no variable") {
    val ast: ASTNode =
      Match(
        optional = false,
        matchMode = MatchMode.default(pos),
        patternForMatch(NodePattern(None, None, None, None) _),
        Seq(),
        None
      ) _

    collector(ast) shouldBe empty
  }
}
