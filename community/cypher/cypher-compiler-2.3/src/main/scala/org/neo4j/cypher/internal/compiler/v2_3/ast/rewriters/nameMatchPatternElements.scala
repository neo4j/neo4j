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
import org.neo4j.cypher.internal.frontend.v2_3.{replace, Rewriter}

case object nameMatchPatternElements extends Rewriter {

  def apply(that: AnyRef): AnyRef = instance(that)

  private val instance: Rewriter = replace(replacer => {
    case expr: Expression =>
      replacer.stop(expr)

    case m: Match =>
      val rewrittenPattern = m.pattern.endoRewrite(nameAllPatternElements.namingRewriter)
      m.copy(pattern = rewrittenPattern)(m.position)

    case astNode =>
      replacer.expand(astNode)
  })
}

case object nameUpdatingClauses extends Rewriter {

  def apply(that: AnyRef): AnyRef = findingRewriter.apply(that)

  private val findingRewriter: Rewriter = replace(replacer => {
    case expr: Expression =>
      replacer.stop(expr)

    case createUnique@CreateUnique(pattern) =>
      val rewrittenPattern = pattern.endoRewrite(nameAllPatternElements.namingRewriter)
      createUnique.copy(pattern = rewrittenPattern)(createUnique.position)

    case create@Create(pattern) =>
      val rewrittenPattern = pattern.endoRewrite(nameAllPatternElements.namingRewriter)
      create.copy(pattern = rewrittenPattern)(create.position)

    case merge@Merge(pattern, _) =>
      val rewrittenPattern = pattern.endoRewrite(nameAllPatternElements.namingRewriter)
      merge.copy(pattern = rewrittenPattern)(merge.position)

    case astNode =>
      replacer.expand(astNode)
  })
}
