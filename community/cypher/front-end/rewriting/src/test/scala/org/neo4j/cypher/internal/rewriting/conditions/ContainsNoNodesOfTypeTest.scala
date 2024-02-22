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
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.UnaliasedReturnItem
import org.neo4j.cypher.internal.expressions.MatchMode
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ContainsNoNodesOfTypeTest extends CypherFunSuite with AstConstructionTestSupport {

  val condition: Any => Seq[String] =
    containsNoNodesOfType[UnaliasedReturnItem]().apply(_)(CancellationChecker.NeverCancelled)

  test("Happy when not finding UnaliasedReturnItem") {
    val ast: ASTNode =
      Match(
        optional = false,
        matchMode = MatchMode.default(pos),
        patternForMatch(NodePattern(None, None, None, None) _),
        Seq(),
        None
      ) _

    condition(ast) should equal(Seq())
  }

  test("Fails when finding UnaliasedReturnItem") {
    val ast: ASTNode = Return(
      false,
      ReturnItems(includeExisting = false, Seq(UnaliasedReturnItem(varFor("foo"), "foo") _)) _,
      None,
      None,
      None
    ) _

    condition(ast) should equal(
      Seq("Expected none but found UnaliasedReturnItem at position line 0, column 0 (offset: 0)")
    )
  }
}
