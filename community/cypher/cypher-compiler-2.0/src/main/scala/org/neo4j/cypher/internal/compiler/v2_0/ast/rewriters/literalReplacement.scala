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
package org.neo4j.cypher.internal.compiler.v2_0.ast.rewriters

import org.neo4j.cypher.internal.compiler.v2_0._
import ast._

object literalReplacement {
  type LiteralReplacements = MutableIdentityMap[Literal, Parameter]

  case class ExtractParameterRewriter(replaceableLiterals: LiteralReplacements) extends Rewriter {
    def apply(that: AnyRef): Option[AnyRef] = rewriter.apply(that)

    private val rewriter: Rewriter = Rewriter.lift {
      case l: Literal =>
        replaceableLiterals.getOrElse(l, l)
    }
  }

  private val literalMatcher: PartialFunction[Any, (LiteralReplacements, LiteralReplacements => LiteralReplacements) => LiteralReplacements] = {
    case _: ast.Match | _: ast.Create | _: ast.CreateUnique | _: ast.Merge | _: ast.SetClause | _: ast.Return | _: ast.With =>
      (acc, children) => children(acc)
    case _: ast.Clause =>
      (acc, _) => acc
    case n: ast.NodePattern =>
      (acc, _) => n.properties.foldt(acc)(literalMatcher)
    case r: ast.RelationshipPattern =>
      (acc, _) => r.properties.foldt(acc)(literalMatcher)
    case l: ast.StringLiteral =>
      (acc, _) => acc + (l -> ast.Parameter(s"  AUTOSTRING${acc.size}")(l.position))
    case l: ast.IntegerLiteral =>
      (acc, _) => acc + (l -> ast.Parameter(s"  AUTOINT${acc.size}")(l.position))
    case l: ast.DoubleLiteral =>
      (acc, _) => acc + (l -> ast.Parameter(s"  AUTODOUBLE${acc.size}")(l.position))
    case l: ast.BooleanLiteral =>
      (acc, _) => acc + (l -> ast.Parameter(s"  AUTOBOOL${acc.size}")(l.position))
  }

  def apply(term: ASTNode): (Rewriter, Map[String, Any]) = {
    val replaceableLiterals = term.foldt(MutableIdentityMap.empty: LiteralReplacements)(literalMatcher)

    (ExtractParameterRewriter(replaceableLiterals), replaceableLiterals.map {
      case (l, p) => (p.name, l.value)
    })
  }
}
