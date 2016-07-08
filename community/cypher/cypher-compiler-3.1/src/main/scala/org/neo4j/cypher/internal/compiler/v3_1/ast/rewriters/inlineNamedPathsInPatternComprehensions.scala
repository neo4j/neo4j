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
package org.neo4j.cypher.internal.compiler.v3_1.ast.rewriters

import org.neo4j.cypher.internal.compiler.v3_1.planner.logical.PatternExpressionPatternElementNamer
import org.neo4j.cypher.internal.frontend.v3_1.ast._
import org.neo4j.cypher.internal.frontend.v3_1.{Rewriter, bottomUp}

case object inlineNamedPathsInPatternComprehensions extends Rewriter {

  private val instance = bottomUp(Rewriter.lift {
    case expr @ PatternComprehension(Some(path), _, _, _) =>
      val (namedExpr, _) = PatternExpressionPatternElementNamer(expr)
      val step: PathStep = projectNamedPaths.patternPartPathExpression(namedExpr.pattern.element)
      val pathExpr = PathExpression(step)(namedExpr.position)
      expr.copy(namedPath = None, predicate = namedExpr.predicate.map(inline(path, _, pathExpr)), projection = inline(path, namedExpr.projection, pathExpr))(expr.position)
  })

  override def apply(v: AnyRef): AnyRef = instance(v)

  def inline(variable: Variable, expr: Expression, replacement: PathExpression): Expression = {
    val rewriter = bottomUp(Rewriter.lift {
      // TODO: This is not scope correct
      case v: Variable if v.name == variable.name => replacement
    })
    expr.endoRewrite(rewriter)
  }

}
