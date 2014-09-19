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

import org.neo4j.cypher.internal.compiler.v2_2._
import ast._
import org.neo4j.cypher.internal.compiler.v2_2.helpers.UnNamedNameGenerator

case object namePatternElements {
  //TODO offset+1 is here for avoiding clashes with legacy stuff in NodePatternConverter
  val namingRewriter: Rewriter = bottomUp(Rewriter.lift {
    case pattern: NodePattern if !pattern.identifier.isDefined =>
      val syntheticName = UnNamedNameGenerator.name(pattern.position.offset + 1)
      pattern.copy(identifier = Some(Identifier(syntheticName)(pattern.position)))(pattern.position)

    case pattern: RelationshipPattern if !pattern.identifier.isDefined  =>
      val syntheticName = UnNamedNameGenerator.name(pattern.position.offset + 1)
      pattern.copy(identifier = Some(Identifier(syntheticName)(pattern.position)))(pattern.position)
  })
}

case object nameMatchPatternElements extends Rewriter {
  def apply(that: AnyRef): Option[AnyRef] = findingRewriter.apply(that)

  private val findingRewriter: Rewriter = bottomUp(Rewriter.lift {
    case m: Match =>
      val rewrittenPattern = m.pattern.endoRewrite(namePatternElements.namingRewriter)
      m.copy(pattern = rewrittenPattern)(m.position)
  })
}

// TODO: When Ronja is the only planner left, move these to nameMatchPatternElements
case object namePatternPredicates extends Rewriter {
  def apply(that: AnyRef): Option[AnyRef] = bottomUp(findingRewriter).apply(that)

  private val findingRewriter: Rewriter = Rewriter.lift {
    case exp: PatternExpression =>
      val rewrittenPattern = exp.pattern.endoRewrite(namePatternElements.namingRewriter)
      exp.copy(pattern = rewrittenPattern)
  }
}

case object nameUpdatingClauses extends Rewriter {
  def apply(that: AnyRef): Option[AnyRef] = findingRewriter.apply(that)

  private val findingRewriter: Rewriter = bottomUp(Rewriter.lift {
    case createUnique@CreateUnique(pattern) => {
      val rewrittenPattern = pattern.endoRewrite(namePatternElements.namingRewriter)
      createUnique.copy(pattern = rewrittenPattern)(createUnique.position)
    }
    case create@Create(pattern) => {
      val rewrittenPattern = pattern.endoRewrite(namePatternElements.namingRewriter)
      create.copy(pattern = rewrittenPattern)(create.position)
    }
    case merge@Merge(pattern, _) => {
      val rewrittenPattern = pattern.endoRewrite(namePatternElements.namingRewriter)
      merge.copy(pattern = rewrittenPattern)(merge.position)
    }
  })
}
