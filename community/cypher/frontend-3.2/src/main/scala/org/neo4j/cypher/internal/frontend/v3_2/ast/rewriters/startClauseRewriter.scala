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
package org.neo4j.cypher.internal.frontend.v3_2.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.phases.{BaseContext, Condition}
import org.neo4j.cypher.internal.frontend.v3_2.{Rewriter, topDown}

case object startClauseRewriter extends StatementRewriter {

  override def description: String = "Rewrites queries using START to equivalent MATCH queries"

  override def postConditions: Set[Condition] = Set.empty
  override def instance(context: BaseContext): Rewriter = topDown(Rewriter.lift {
    case start@Start(items, where) =>
      val newPredicates = items.collect {
        //We can safely ignore AllNodes here, i.e. nodes(*) since that is corresponding to
        //no predicate on the identifier

        //START n=nodes(1,5,7)....
        case n@NodeByIds(variable, ids) =>
          val pos = n.position
          val invocation = FunctionInvocation(FunctionName(functions.Id.name)(pos), variable.copyId)(pos)
          val in = In(invocation, ListLiteral(ids)(pos))(pos)
          in
      }

      val hints = items.collect {
        case hint: LegacyIndexHint => hint
      }
      val nodes = items.collect {
        case n: NodeStartItem => NodePattern(Some(n.variable.copyId), Seq.empty, None)(start.position)
      }

      val allPredicates = (newPredicates ++ where.map(_.expression)).toSet
      val newWhere =
        if (allPredicates.isEmpty) None
        else if (allPredicates.size == 1) Some(Where(allPredicates.head)(start.position))
        else Some(Where(Ands(allPredicates)(start.position))(start.position))
      Match(optional = false, Pattern(nodes.map(EveryPath))(start.position), hints, newWhere)(start.position)
  })

}
