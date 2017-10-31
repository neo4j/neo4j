/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.v3_4.ast.rewriters

import org.neo4j.cypher.internal.util.v3_4._
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.v3_4.expressions.{Expression, Variable}

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

  def apply(that: AnyRef): AnyRef = instance.apply(that)

  private val clauseRewriter: (Clause => Seq[Clause]) = {
    case clause @ Return(_, ri@ReturnItems(_, items), _, None, _, _, _) =>
      val aliasedItems = items.map({
        case i: AliasedReturnItem =>
          i
        case i =>
          val newPosition = i.expression.position.bumped()
          AliasedReturnItem(i.expression, Variable(i.name)(newPosition))(i.position)
      })
      Seq(
        clause.copy(returnItems = ri.copy(items = aliasedItems)(ri.position))(clause.position)
      )

    case clause @ Return(distinct, ri: ReturnItems, gri, orderBy, skip, limit, _) =>
      clause.verifyOrderByAggregationUse((s,i) => throw mkException(s,i))
      var rewrites = Map[Expression, Variable]()

      val (aliasProjection, finalProjection) = ri.items.map {
        i =>
          val returnColumn = i.alias match {
            case Some(alias) => alias
            case None        => Variable(i.name)(i.expression.position.bumped())
          }

          val newVariable = Variable(FreshIdNameGenerator.name(i.expression.position))(i.expression.position)

          rewrites = rewrites + (returnColumn -> newVariable)
          rewrites = rewrites + (i.expression -> newVariable)

          (AliasedReturnItem(i.expression, newVariable)(i.position), AliasedReturnItem(newVariable.copyId, returnColumn)(i.position))
      }.unzip

      val newOrderBy = orderBy.endoRewrite(topDown(Rewriter.lift {
        case exp: Expression if rewrites.contains(exp) => rewrites(exp).copyId
      }))

      val introducedVariables = if (ri.includeExisting) aliasProjection.map(_.variable.name).toSet else Set.empty[String]

      Seq(
        With(distinct = distinct, returnItems = ri.copy(items = aliasProjection)(ri.position), gri.getOrElse(PassAllGraphReturnItems(clause.position)),
          orderBy = newOrderBy, skip = skip, limit = limit, where = None)(clause.position),
        Return(distinct = false, returnItems = ri.copy(items = finalProjection)(ri.position), gri,
          orderBy = None, skip = None, limit = None, excludedNames = introducedVariables)(clause.position)
      )

    case clause =>
      Seq(clause)
  }

  private val instance: Rewriter = bottomUp(Rewriter.lift {
    case query @ SingleQuery(clauses) =>
      query.copy(clauses = clauses.flatMap(clauseRewriter))(query.position)
  })
}
