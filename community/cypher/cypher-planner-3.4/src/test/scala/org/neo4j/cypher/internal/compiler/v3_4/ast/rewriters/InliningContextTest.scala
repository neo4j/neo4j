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

import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.expressions._

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
