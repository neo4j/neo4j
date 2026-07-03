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
package org.neo4j.cypher.internal.rewriting

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.rewriting.rewriters.InliningContext
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class InliningContextTest extends CypherFunSuite with AstConstructionTestSupport {

  private val identN = varFor("n")
  private val identM = varFor("m")
  private val identA = varFor("a")

  private val mapN = Map[LogicalVariable, Expression](identN -> nullLiteral)
  private val mapM = Map[LogicalVariable, Expression](identM -> nullLiteral)
  private val mapA = Map[LogicalVariable, Expression](identA -> nullLiteral)

  private val mapAtoN = Map[LogicalVariable, Expression](identA -> identN)

  test("update projections on enterQueryPart") {
    val ctx = InliningContext(mapM).enterQueryPart(mapN)

    ctx.projections should equal(mapM ++ mapN)
  }

  test("inline expressions on enterQueryPart") {
    val ctx = InliningContext(mapN).enterQueryPart(mapAtoN)

    ctx.projections should equal(mapN ++ mapA)
  }

  test("throw assertiona error when new projections use an already seen variable") {
    intercept[AssertionError](InliningContext().enterQueryPart(mapN).enterQueryPart(mapN))
  }

  test("ignore new projections when spoilVariable is called") {
    val ctx = InliningContext(mapN).spoilVariable(identN)

    ctx.projections should equal(Map.empty)
  }

  test("should inline aliases into node patterns") {
    val ctx = InliningContext(mapAtoN)

    val expr: NodePattern = NodePattern(Some(identA), None, None, None) _

    expr.endoRewrite(ctx.patternRewriter).variable should equal(Some(identN))
  }

  test("should inline aliases into relationship patterns") {
    val ctx = InliningContext(mapAtoN)

    val expr: RelationshipPattern =
      RelationshipPattern(Some(identA), None, None, None, None, SemanticDirection.OUTGOING) _

    expr.endoRewrite(ctx.patternRewriter).variable should equal(Some(identN))
  }
}
