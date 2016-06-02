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

case class bottomUpExpressions(rewriters: Rewriter*) extends Rewriter {
  import Rewritable._

  def apply(term: AnyRef): Some[AnyRef] = Some(term match {
    case _: ast.Match | _: ast.Create | _: ast.CreateUnique | _: ast.Merge | _: ast.SetClause | _: ast.Return | _: ast.With =>
      term.dup(t => this.apply(t).get)

    case _: ast.Clause =>
      term

    case n: ast.NodePattern =>
      val rewrittenProperties = n.properties.map(t => this.apply(t).get).asInstanceOf[Option[ast.Expression]]
      if (rewrittenProperties == n.properties)
        n
      else
        n.copy(properties = rewrittenProperties)(n.position)

    case n: ast.RelationshipPattern =>
      val rewrittenProperties = n.properties.map(t => this.apply(t).get).asInstanceOf[Option[ast.Expression]]
      if (rewrittenProperties == n.properties)
        n
      else
        n.copy(properties = rewrittenProperties)(n.position)

    case _: ast.Expression =>
      rewriters.foldLeft(term.dup(t => this.apply(t).get)) {
        (t, r) => t.rewrite(r)
      }

    case _ =>
      term.dup(t => this.apply(t).get)
  })
}
