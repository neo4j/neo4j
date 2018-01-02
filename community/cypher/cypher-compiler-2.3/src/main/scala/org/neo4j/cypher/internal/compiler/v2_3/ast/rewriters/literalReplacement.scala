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

import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.{IdentityMap, Rewriter, ast, bottomUp}

object literalReplacement {
  type LiteralReplacements = IdentityMap[Literal, Parameter]

  case class ExtractParameterRewriter(replaceableLiterals: LiteralReplacements) extends Rewriter {
    def apply(that: AnyRef): AnyRef = bottomUp(rewriter).apply(that)

    private val rewriter: Rewriter = Rewriter.lift {
      case l: Literal =>
        replaceableLiterals.getOrElse(l, l)
    }
  }

  private val literalMatcher: PartialFunction[Any, (LiteralReplacements, LiteralReplacements => LiteralReplacements) => LiteralReplacements] = {
    case _: ast.Match | _: ast.Create | _: ast.CreateUnique | _: ast.Merge | _: ast.SetClause | _: ast.Return | _: ast.With =>
      (acc, children) => children(acc)
    case _: ast.Clause | _: ast.PeriodicCommitHint | _: ast.Limit =>
      (acc, _) => acc
    case n: ast.NodePattern =>
      (acc, _) => n.properties.treeFold(acc)(literalMatcher)
    case r: ast.RelationshipPattern =>
      (acc, _) => r.properties.treeFold(acc)(literalMatcher)
    case ast.ContainerIndex(_, _: ast.StringLiteral) =>
      (acc, _) => acc
    case l: ast.StringLiteral =>
      (acc, _) => if (acc.contains(l)) acc else acc + (l -> ast.Parameter(s"  AUTOSTRING${acc.size}")(l.position))
    case l: ast.IntegerLiteral =>
      (acc, _) =>
        if (acc.contains(l)) acc else
        acc + (l -> ast.Parameter(s"  AUTOINT${acc.size}")(l.position))
    case l: ast.DoubleLiteral =>
      (acc, _) => if (acc.contains(l)) acc else acc + (l -> ast.Parameter(s"  AUTODOUBLE${acc.size}")(l.position))
    case l: ast.BooleanLiteral =>
      (acc, _) => if (acc.contains(l)) acc else acc + (l -> ast.Parameter(s"  AUTOBOOL${acc.size}")(l.position))
  }

  def apply(term: ASTNode): (Rewriter, Map[String, Any]) = {
    // TODO: Replace with .exists
    val containsParameter: Boolean = term.treeFold(false) {
      case term: Parameter =>
        (acc, _) => true
      case _ =>
        (acc, children) => if (acc) true else children(acc)
    }

    if (containsParameter) {
      (Rewriter.noop, Map.empty)
    } else {
      val replaceableLiterals = term.treeFold(IdentityMap.empty: LiteralReplacements)(literalMatcher)

      val extractedParams: Map[String, AnyRef] = replaceableLiterals.map {
        case (l, p) => (p.name, l.value)
      }

      (ExtractParameterRewriter(replaceableLiterals), extractedParams)
    }
  }
}
