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
import org.neo4j.cypher.internal.ir.v3_2.UnNamedNameGenerator

case object nameAllPatternElements extends Rewriter {

  override def apply(in: AnyRef): AnyRef = namingRewriter.apply(in)

  val namingRewriter: Rewriter = bottomUp(Rewriter.lift {
    case pattern: NodePattern if pattern.variable.isEmpty =>
      val syntheticName = UnNamedNameGenerator.name(pattern.position.bumped())
      pattern.copy(variable = Some(Variable(syntheticName)(pattern.position)))(pattern.position)

    case pattern: RelationshipPattern if pattern.variable.isEmpty  =>
      val syntheticName = UnNamedNameGenerator.name(pattern.position.bumped())
      pattern.copy(variable = Some(Variable(syntheticName)(pattern.position)))(pattern.position)
  })
}
