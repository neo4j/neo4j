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
  case class ExtractParameterRewriter(replaceableLiterals: Map[(Any, InputPosition), Parameter]) extends Rewriter {
    def apply(that: AnyRef): Option[AnyRef] = rewriter.apply(that)

    private val rewriter: Rewriter = Rewriter.lift {
      case l: Literal =>
        replaceableLiterals.get((l.value, l.position)).getOrElse(l)
    }
  }

  private val literalMatcher: PartialFunction[Any, Map[(Any, InputPosition), ast.Parameter] => Map[(Any, InputPosition), ast.Parameter]] = {
    case l: ast.StringLiteral => m => m + ((l.value, l.position) -> ast.Parameter(s"  AUTOSTRING${m.size}")(l.position))
    case l: ast.IntegerLiteral => m => m + ((l.value, l.position) -> ast.Parameter(s"  AUTOINT${m.size}")(l.position))
    case l: ast.DoubleLiteral => m => m + ((l.value, l.position) -> ast.Parameter(s"  AUTODOUBLE${m.size}")(l.position))
    case l: ast.BooleanLiteral => m => m + ((l.value, l.position) -> ast.Parameter(s"  AUTOBOOL${m.size}")(l.position))
  }

  def apply(term: ASTNode): (Rewriter, Map[String, Any]) = {
    val replaceableLiterals = term.foldt(Map.empty[(Any,InputPosition), ast.Parameter]) {
      case u: ast.SetClause =>
        (acc, _) => u.fold(acc)(literalMatcher)
      case r: ast.Return =>
        (acc, _) => r.fold(acc)(literalMatcher)
      case w: ast.With =>
        (acc, _) => w.fold(acc)(literalMatcher)
      case _: ast.Match | _: ast.Create | _: ast.CreateUnique | _: ast.Merge =>
        (acc, children) => children(acc)
      case _: ast.Clause =>
        (acc, _) => acc
      case n: ast.NodePattern =>
        (acc, _) => n.properties.fold(acc)(_.fold(acc)(literalMatcher))
      case r: ast.RelationshipPattern =>
        (acc, _) => r.properties.fold(acc)(_.fold(acc)(literalMatcher))
      case w: ast.Where =>
        (acc, _) => w.fold(acc)(literalMatcher)
    }

    (ExtractParameterRewriter(replaceableLiterals), replaceableLiterals.map {
      case (l, p) => (p.name, l._1)
    })
  }
}
