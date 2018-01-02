/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.ast.rewriters

import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class InliningContextTest extends CypherFunSuite with AstConstructionTestSupport {

  val identN = ident("n")
  val identM = ident("m")
  val identA = ident("a")
  val astNull: Null = Null()_

  val mapN = Map(identN -> astNull)
  val mapM = Map(identM -> astNull)
  val mapA = Map(identA -> astNull)

  val mapAtoN = Map(identA -> identN)

  test("update projections on enterQueryPart") {
    val ctx = InliningContext(mapM).enterQueryPart(mapN)

    ctx.projections should equal(mapM ++ mapN)
  }

  test("inline expressions on enterQueryPart") {
    val ctx = InliningContext(mapN).enterQueryPart(mapAtoN)

    ctx.projections should equal(mapN ++ mapA)
  }

  test("throw assertiona error when new projections use an already seen identifier") {
    intercept[AssertionError](InliningContext().enterQueryPart(mapN).enterQueryPart(mapN))
  }

  test("ignore new projections when spoilIdentifier is called") {
    val ctx = InliningContext(mapN).spoilIdentifier(identN)

    ctx.projections should equal(Map.empty)
  }

  test("should inline aliases into node patterns") {
    val ctx = InliningContext(mapAtoN)

    val expr: NodePattern = NodePattern(Some(identA), Seq(), None, naked = false)_

    expr.endoRewrite(ctx.patternRewriter).identifier should equal(Some(identN))
  }

  test("should inline aliases into relationship patterns") {
    val ctx = InliningContext(mapAtoN)

    val expr: RelationshipPattern = RelationshipPattern(Some(identA), optional = false, Seq(), None, None, SemanticDirection.OUTGOING)_

    expr.endoRewrite(ctx.patternRewriter).identifier should equal(Some(identN))
  }
}
