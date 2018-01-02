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

import org.neo4j.cypher.internal.compiler.v2_3.helpers.FreshIdNameGenerator
import org.neo4j.cypher.internal.frontend.v2_3._
import org.neo4j.cypher.internal.frontend.v2_3.ast._

/**
 * This rewriter makes sure that all return items in a RETURN clauses are aliased, and moves
 * any ORDER BY to a preceding WITH clause
 *
 * Example:
 *
 * MATCH (n)
 * RETURN n.foo AS foo, n.bar ORDER BY foo
 *
 * This rewrite will change the query to:
 *
 * MATCH (n)
 * WITH n.foo AS `  FRESHIDxx`, n.bar AS `  FRESHIDnn` ORDER BY `  FRESHIDxx`
 * RETURN `  FRESHIDxx` AS foo, `  FRESHIDnn` AS `n.bar`
 */
case class normalizeReturnClauses(mkException: (String, InputPosition) => CypherException) extends Rewriter {

  def apply(that: AnyRef): AnyRef = bottomUp(instance).apply(that)

  private val clauseRewriter: (Clause => Seq[Clause]) = {
    case clause @ Return(_, ri, None, _, _, _) =>
      val aliasedItems = ri.items.map({
        case i: AliasedReturnItem =>
          i
        case i =>
          val newPosition = i.expression.position.bumped()
          AliasedReturnItem(i.expression, Identifier(i.name)(newPosition))(i.position)
      })
      Seq(
        clause.copy(returnItems = ri.copy(items = aliasedItems)(ri.position))(clause.position)
      )

    case clause @ Return(distinct, ri, orderBy, skip, limit, _) =>
      clause.verifyOrderByAggregationUse((s,i) => throw mkException(s,i))
      var rewrites = Map[Expression, Identifier]()

      val (aliasProjection, finalProjection) = ri.items.map {
        i =>
          val returnColumn = i.alias match {
            case Some(alias) => alias
            case None        => Identifier(i.name)(i.expression.position.bumped())
          }

          val newIdentifier = Identifier(FreshIdNameGenerator.name(i.expression.position))(i.expression.position)

          rewrites = rewrites + (returnColumn -> newIdentifier)
          rewrites = rewrites + (i.expression -> newIdentifier)

          (AliasedReturnItem(i.expression, newIdentifier)(i.position), AliasedReturnItem(newIdentifier.copyId, returnColumn)(i.position))
      }.unzip

      val newOrderBy = orderBy.endoRewrite(topDown(Rewriter.lift {
        case exp: Expression if rewrites.contains(exp) => rewrites(exp).copyId
      }))

      val introducedVariables = if (ri.includeExisting) aliasProjection.map(_.identifier.name).toSet else Set.empty[String]

      Seq(
        With(distinct = distinct, returnItems = ri.copy(items = aliasProjection)(ri.position),
          orderBy = newOrderBy, skip = skip, limit = limit, where = None)(clause.position),
        Return(distinct = false, returnItems = ri.copy(items = finalProjection)(ri.position),
          orderBy = None, skip = None, limit = None, excludedNames = introducedVariables)(clause.position)
      )

    case clause =>
      Seq(clause)
  }

  private val instance: Rewriter = Rewriter.lift {
    case query @ SingleQuery(clauses) =>
      query.copy(clauses = clauses.flatMap(clauseRewriter))(query.position)
  }
}
