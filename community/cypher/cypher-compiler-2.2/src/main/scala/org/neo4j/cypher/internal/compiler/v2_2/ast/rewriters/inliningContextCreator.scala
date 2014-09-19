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

import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2._

object inliningContextCreator extends (ast.Statement => InliningContext) {

  def apply(input: ast.Statement): InliningContext = {
    input.treeFold(InliningContext()) {
      case withClause: With if !withClause.distinct =>
        (context, children) => children(context.enterQueryPart(aliasedReturnItems(withClause.returnItems.items)))

      case sortItem: SortItem =>
        (context, children) => children(context.spoilIdentifier(sortItem.expression.asInstanceOf[Identifier]))

      case NodePattern(Some(identifier), _, _, _) =>
        (context, children) =>
          if (context.alias(identifier).isEmpty) children(context.spoilIdentifier(identifier)) else children(context)

      case RelationshipPattern(Some(identifier), _, _, _, _, _) =>
        (context, children) =>
          if (context.alias(identifier).isEmpty) children(context.spoilIdentifier(identifier)) else children(context)
    }
  }

  private def aliasedReturnItems(items: Seq[ReturnItem]): Map[Identifier, Expression] =
    items.collect { case AliasedReturnItem(expr, ident) => ident -> expr }.toMap
}
