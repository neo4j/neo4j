/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_0.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_0.ast._
import org.neo4j.cypher.internal.frontend.v3_0.symbols._
import org.neo4j.cypher.internal.frontend.v3_0.{IdentityMap, Rewriter, ast, bottomUp}

object literalReplacement {
  type LiteralReplacements = IdentityMap[Literal, Parameter]

  case class ExtractParameterRewriter(replaceableLiterals: LiteralReplacements) extends Rewriter {
    def apply(that: AnyRef): AnyRef = rewriter.apply(that)

    private val rewriter: Rewriter = bottomUp(Rewriter.lift {
      case l: Literal =>
        replaceableLiterals.getOrElse(l, l)
    })
  }

  private val literalMatcher: PartialFunction[Any, LiteralReplacements => (LiteralReplacements, Option[LiteralReplacements => LiteralReplacements])] = {
    case _: ast.Match |
         _: ast.Create |
         _: ast.CreateUnique |
         _: ast.Merge |
         _: ast.SetClause |
         _: ast.Return |
         _: ast.With |
         _: ast.CallClause =>
      acc => (acc, Some(identity))
    case _: ast.Clause |
         _: ast.PeriodicCommitHint |
         _: ast.Limit =>
      acc => (acc, None)
    case n: ast.NodePattern =>
      acc => (n.properties.treeFold(acc)(literalMatcher), None)
    case r: ast.RelationshipPattern =>
      acc => (r.properties.treeFold(acc)(literalMatcher), None)
    case ast.ContainerIndex(_, _: ast.StringLiteral) =>
      acc => (acc, None)
    case l: ast.StringLiteral =>
      acc => (acc + (l -> ast.Parameter(s"  AUTOSTRING${acc.size}", CTString)(l.position)), None)
    case l: ast.IntegerLiteral =>
      acc => (acc + (l -> ast.Parameter(s"  AUTOINT${acc.size}", CTInteger)(l.position)), None)
    case l: ast.DoubleLiteral =>
      acc => (acc + (l -> ast.Parameter(s"  AUTODOUBLE${acc.size}", CTFloat)(l.position)), None)
    case l: ast.BooleanLiteral =>
      acc => (acc + (l -> ast.Parameter(s"  AUTOBOOL${acc.size}", CTBoolean)(l.position)), None)
  }

  def apply(term: ASTNode): (Rewriter, Map[String, Any]) = {
    // TODO: Replace with .exists
    val containsParameter: Boolean = term.treeFold(false) {
      case term: Parameter =>
        acc => (true, None)
      case _ =>
        acc => (acc, if (acc) None else Some(identity))
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
