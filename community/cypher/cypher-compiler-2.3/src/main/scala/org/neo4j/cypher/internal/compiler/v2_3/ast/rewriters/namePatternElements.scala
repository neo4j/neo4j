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
import org.neo4j.cypher.internal.compiler.v2_3.helpers.UnNamedNameGenerator
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.PatternExpressionPatternElementNamer
import org.neo4j.cypher.internal.frontend.v2_3.{Rewriter, bottomUp}

case object nameAllPatternElements extends Rewriter {

  override def apply(in: AnyRef): AnyRef = bottomUp(namingRewriter).apply(in)

  val namingRewriter: Rewriter = bottomUp(Rewriter.lift {
    case pattern: NodePattern if !pattern.identifier.isDefined =>
      val syntheticName = UnNamedNameGenerator.name(pattern.position.bumped())
      pattern.copy(identifier = Some(Identifier(syntheticName)(pattern.position)))(pattern.position)

    case pattern: RelationshipPattern if !pattern.identifier.isDefined  =>
      val syntheticName = UnNamedNameGenerator.name(pattern.position.bumped())
      pattern.copy(identifier = Some(Identifier(syntheticName)(pattern.position)))(pattern.position)
  })
}

case object namePatternPredicatePatternElements extends Rewriter {

  override def apply(in: AnyRef): AnyRef = bottomUp(instance).apply(in)

  val instance: Rewriter = Rewriter.lift {
    case expr: PatternExpression =>
      val (rewrittenExpr, _) = PatternExpressionPatternElementNamer(expr)
      rewrittenExpr
  }
}
