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
package org.neo4j.cypher.internal.compiler.v2_1.ast.rewriters

import org.neo4j.cypher.internal.compiler.v2_1._
import ast._
import org.neo4j.cypher.internal.compiler.v2_1.helpers.UnNamedNameGenerator

object nameMatchPatternElements extends Rewriter {
  def apply(that: AnyRef): Option[AnyRef] = bottomUp(findingRewriter).apply(that)

  private val namingRewriter: Rewriter = Rewriter.lift {
    case pattern: NodePattern if !pattern.identifier.isDefined =>
      val syntheticName = UnNamedNameGenerator.name(pattern.position.offset + 1)
      pattern.copy(identifier = Some(Identifier(syntheticName)(pattern.position)))(pattern.position)

    // TODO: Don't exclude varlength relationships (currently need to be for legacy conversion)
    case pattern: RelationshipPattern if !pattern.identifier.isDefined && !pattern.length.isDefined =>
      val syntheticName = UnNamedNameGenerator.name(pattern.position.offset)
      pattern.copy(identifier = Some(Identifier(syntheticName)(pattern.position)))(pattern.position)
  }

  private val findingRewriter: Rewriter = Rewriter.lift {
    case m: Match =>
      val rewrittenPattern = m.pattern.endoRewrite(bottomUp(namingRewriter))
      m.copy(pattern = rewrittenPattern)(m.position)
  }
}

// TODO: When Ronja is the only planner left, move these to nameMatchPatternElements
object nameVarLengthRelationships extends Rewriter {
  def apply(that: AnyRef): Option[AnyRef] = bottomUp(findingRewriter).apply(that)

  private val namingRewriter: Rewriter = Rewriter.lift {
    case pattern: RelationshipPattern if !pattern.identifier.isDefined && pattern.length.isDefined =>
      val syntheticName = UnNamedNameGenerator.name(pattern.position.offset)
      pattern.copy(identifier = Some(Identifier(syntheticName)(pattern.position)))(pattern.position)
  }

  private val findingRewriter: Rewriter = Rewriter.lift {
    case m: Match =>
      val rewrittenPattern = m.pattern.endoRewrite(bottomUp(namingRewriter))
      m.copy(pattern = rewrittenPattern)(m.position)
  }
}

// TODO: When Ronja is the only planner left, move these to nameMatchPatternElements
object namePatternPredicates extends Rewriter {
  def apply(that: AnyRef): Option[AnyRef] = bottomUp(findingRewriter).apply(that)

  private val namingRewriter: Rewriter = Rewriter.lift {
    case pattern: NodePattern if !pattern.identifier.isDefined =>
      val syntheticName = UnNamedNameGenerator.name(pattern.position.offset + 1)
      pattern.copy(identifier = Some(Identifier(syntheticName)(pattern.position)))(pattern.position)

    case pattern: RelationshipPattern if !pattern.identifier.isDefined =>
      val syntheticName = UnNamedNameGenerator.name(pattern.position.offset)
      pattern.copy(identifier = Some(Identifier(syntheticName)(pattern.position)))(pattern.position)
  }

  private val findingRewriter: Rewriter = Rewriter.lift {
    case exp: PatternExpression =>
      val rewrittenPattern = exp.pattern.endoRewrite(bottomUp(namingRewriter))
      exp.copy(pattern = rewrittenPattern)
  }
}
