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
import org.neo4j.cypher.internal.v3_5.rewriting.rewriters.InliningContext
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_5.expressions.{NodePattern, RelationshipPattern}

class InliningContextTest extends CypherFunSuite with AstConstructionTestSupport {

  val identN = varFor("n")
  val identM = varFor("m")
  val identA = varFor("a")
  val astNull: Null = Null()_

  val mapN = Map[LogicalVariable, Expression](identN -> astNull)
  val mapM = Map[LogicalVariable, Expression](identM -> astNull)
  val mapA = Map[LogicalVariable, Expression](identA -> astNull)

  val mapAtoN = Map[LogicalVariable, Expression](identA -> identN)

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

    val expr: NodePattern = NodePattern(Some(identA), Seq(), None)_

    expr.endoRewrite(ctx.patternRewriter).variable should equal(Some(identN))
  }

  test("should inline aliases into relationship patterns") {
    val ctx = InliningContext(mapAtoN)

    val expr: RelationshipPattern = RelationshipPattern(Some(identA), Seq(), None, None, SemanticDirection.OUTGOING)_

    expr.endoRewrite(ctx.patternRewriter).variable should equal(Some(identN))
  }
}
