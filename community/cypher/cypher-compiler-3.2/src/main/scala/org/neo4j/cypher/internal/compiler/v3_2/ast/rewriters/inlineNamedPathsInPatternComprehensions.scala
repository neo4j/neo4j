/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.{Rewriter, bottomUp}

case object inlineNamedPathsInPatternComprehensions extends Rewriter {

  private val instance = bottomUp(Rewriter.lift {
    case expr @ PatternComprehension(Some(path), pattern, predicate, projection, _) =>
      val patternElement = pattern.element
      expr.copy(
        namedPath = None,
        predicate = predicate.map(_.inline(path, patternElement)),
        projection = projection.inline(path, patternElement)
      )(expr.position)
  })

  private implicit final class InliningExpression(val expr: Expression) extends AnyVal {
    def inline(path: Variable, patternElement: PatternElement) =
      expr.copyAndReplace(path) by {
        PathExpression(projectNamedPaths.patternPartPathExpression(patternElement))(expr.position)
      }
  }

  override def apply(v: AnyRef): AnyRef = instance(v)
}
