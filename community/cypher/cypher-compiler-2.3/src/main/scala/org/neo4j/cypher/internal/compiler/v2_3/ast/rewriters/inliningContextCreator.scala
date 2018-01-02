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
import org.neo4j.cypher.internal.frontend.v2_3._

object inliningContextCreator extends (ast.Statement => InliningContext) {

  def apply(input: ast.Statement): InliningContext = {
    input.treeFold(InliningContext()) {
      // We cannot inline expressions in a DISTINCT with clause, projecting the result of the expression
      // would change the result of the distinctification
      case withClause: With if !withClause.distinct =>
        (context, children) =>
          children(context.enterQueryPart(aliasedReturnItems(withClause.returnItems.items)))

      // When just passing an identifier through a WITH, do not count the identifier as used. This case shortcuts the
      // tree folding so the identifiers are not tracked.
      case AliasedReturnItem(Identifier(n1), alias@Identifier(n2)) if n1 == n2 =>
        (context, children) =>
          context

      case identifier: Identifier =>
        (context, children) =>
          children(context.trackUsageOfIdentifier(identifier))

      // When an identifier is used in ORDER BY, it should never be inlined
      case sortItem: SortItem =>
        (context, children) =>
          children(context.spoilIdentifier(sortItem.expression.asInstanceOf[Identifier]))

      // Do not inline pattern identifiers, unless they are clean aliases of previous identifiers
      case NodePattern(Some(identifier), _, _, _) =>
        (context, children) =>
          if (context.isAliasedIdentifier(identifier))
            children(context)
          else
            children(context.spoilIdentifier(identifier))

      case RelationshipPattern(Some(identifier), _, _, _, _, _) =>
        (context, children) =>
          if (context.isAliasedIdentifier(identifier))
            children(context)
          else
            children(context.spoilIdentifier(identifier))
    }
  }

  private def aliasedReturnItems(items: Seq[ReturnItem]): Map[Identifier, Expression] =
    items.collect { case AliasedReturnItem(expr, ident) => ident -> expr }.toMap
}
