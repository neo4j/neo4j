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
package org.neo4j.cypher.internal.compiler.v2_3.ast.convert.plannerQuery

import org.neo4j.cypher.internal.compiler.v2_3.ast.convert.plannerQuery.ClauseConverters._
import org.neo4j.cypher.internal.compiler.v2_3.planner._
import org.neo4j.cypher.internal.frontend.v2_3.{Foldable, ast}
import org.neo4j.cypher.internal.frontend.v2_3.ast._

object StatementConverters {

  implicit class SingleQueryPartConverter(val q: SingleQuery) {
    def asPlannerQueryBuilder: PlannerQueryBuilder =
      q.clauses.foldLeft(PlannerQueryBuilder.empty) {
        case (acc, clause) => clause.addToLogicalPlanInput(acc)
      }
  }

  val NODE_BLACKLIST: Set[Class[_ <: ASTNode]] = Set(
    classOf[And],
    classOf[Or],
    // classOf[ReturnAll],
    classOf[UnaliasedReturnItem]
  )

  import Foldable._
  def findBlacklistedNodes(node: AnyRef): Seq[ASTNode] = {
    node.treeFold(Seq.empty[ASTNode]) {
      case node: ASTNode if NODE_BLACKLIST.contains(node.getClass) =>
        (acc, children)  => children(acc :+ node)
    }
  }

  implicit class QueryConverter(val query: Query) {
    def asUnionQuery: UnionQuery = {
      val nodes = findBlacklistedNodes(query)
      require(nodes.isEmpty, "Found a blacklisted AST node: " + nodes.head.toString)

      query match {
        case Query(None, queryPart: SingleQuery) =>
          val builder = queryPart.asPlannerQueryBuilder
          UnionQuery(Seq(builder.build()), distinct = false, builder.returns)

        case Query(None, u: ast.Union) =>
          val queries: Seq[SingleQuery] = u.unionedQueries
          val distinct = u match {
            case _: UnionAll => false
            case _: UnionDistinct => true
          }
          val plannedQueries: Seq[PlannerQueryBuilder] = queries.reverseMap(x => x.asPlannerQueryBuilder)
          //UNION requires all queries to return the same identifiers
          assert(plannedQueries.nonEmpty)
          val returns = plannedQueries.head.returns
          assert(plannedQueries.forall(_.returns == returns))

          UnionQuery(plannedQueries.map(_.build()), distinct, returns)

        case _ =>
          throw new CantHandleQueryException
      }
    }
  }
}
