/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.ast.rewriters

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.planner.CantHandleQueryException
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.compiler.v2_2.ast.Expression.SemanticContext

class InliningContextTest extends CypherFunSuite with AstConstructionTestSupport {

  val identN = ident("n")
  val identM = ident("m")
  val identA = ident("a")
  val astNull: Null = Null()_

  val mapN = Map(identN -> astNull)
  val mapM = Map(identM -> astNull)
  val mapA = Map(identA -> astNull)

  val mapAtoN = Map(identA -> identN)

  test("update seen identifiers on enterQueryPart") {
    val ctx = InliningContext().enterQueryPart(mapN)

    ctx.seenIdentifiers should equal(Set(identN))
  }

  test("update seen identifiers on spoilIdentifier") {
    val ctx = InliningContext().spoilIdentifier(Identifier("n")_)

    ctx.seenIdentifiers should equal(Set(identN))
  }

  test("update projections on enterQueryPart") {
    val ctx = InliningContext(mapM).enterQueryPart(mapN)

    ctx.projections should equal(mapM ++ mapN)
  }

  test("inline expressions on enterQueryPart") {
    val ctx = InliningContext(mapN).enterQueryPart(mapAtoN)

    ctx.projections should equal(mapN ++ mapA)
  }

  test("ignore new projections if they use an already seen identifier") {
    val ctx = InliningContext().enterQueryPart(mapN).enterQueryPart(mapN)

    ctx.projections should equal(Map.empty)
  }

  test("ignore new projections when spoilIdentifier is called") {
    val ctx = InliningContext(mapN).spoilIdentifier(identN)

    ctx.projections should equal(Map.empty)
  }

  test("should throw CantHandleQueryException when encountering ScopeIntroducingExpression that uses seen identifier") {
    val ctx = InliningContext().enterQueryPart(mapN)

    case object expr extends Expression with ScopeIntroducingExpression {
      val identifier = identN
      val position = pos
      def semanticCheck(ctx: SemanticContext) = ???
    }

    evaluating { ctx.identifierRewriter(expr) } should produce[CantHandleQueryException]
  }

  test("should inline aliases into node patterns") {
    val ctx = InliningContext(mapAtoN)

    val expr: NodePattern = NodePattern(Some(identA), Seq(), None, naked = false)_

    expr.endoRewrite(ctx.patternRewriter).identifier should equal(Some(identN))
  }

  test("should inline aliases into relationship patterns") {
    val ctx = InliningContext(mapAtoN)

    val expr: RelationshipPattern = RelationshipPattern(Some(identA), optional = false, Seq(), None, None, Direction.OUTGOING)_

    expr.endoRewrite(ctx.patternRewriter).identifier should equal(Some(identN))
  }
}
