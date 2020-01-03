/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery

import org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery.ClauseConverters._
import org.neo4j.cypher.internal.ir.{PeriodicCommit, SinglePlannerQuery, PlannerQueryPart, PlannerQuery, UnionQuery}
import org.neo4j.cypher.internal.v4_0.ast
import org.neo4j.cypher.internal.v4_0.ast._
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v4_0.expressions.{And, Or, Pattern, PatternPart}
import org.neo4j.cypher.internal.v4_0.util.{ASTNode, InputPosition}
import org.neo4j.exceptions.InternalException

import scala.collection.mutable.ArrayBuffer

object StatementConverters {
  import org.neo4j.cypher.internal.v4_0.util.Foldable._

  def toPlannerQueryBuilder(q: SingleQuery, semanticTable: SemanticTable): PlannerQueryBuilder =
    flattenCreates(q.clauses).foldLeft(PlannerQueryBuilder(semanticTable)) {
      case (acc, clause) => addToLogicalPlanInput(acc, clause)
    }

  private val NODE_BLACKLIST: Set[Class[_ <: ASTNode]] = Set(
    classOf[And],
    classOf[Or],
    classOf[UnaliasedReturnItem],
    classOf[Start],
    classOf[UnionAll],
    classOf[UnionDistinct]
  )


  private def findBlacklistedNodes(node: AnyRef): Seq[ASTNode] = {
    node.treeFold(Seq.empty[ASTNode]) {
      case node: ASTNode if NODE_BLACKLIST.contains(node.getClass) =>
        acc => (acc :+ node, Some(identity))
    }
  }

  def toPlannerQuery(query: Query, semanticTable: SemanticTable): PlannerQuery = {
    val plannerQueryPart = toPlannerQueryPart(query.part, semanticTable)
    PlannerQuery(plannerQueryPart, PeriodicCommit(query.periodicCommitHint))
  }

  def toPlannerQueryPart(queryPart: QueryPart, semanticTable: SemanticTable): PlannerQueryPart = {
    val nodes = findBlacklistedNodes(queryPart)
    require(nodes.isEmpty, "Found a blacklisted AST node: " + nodes.head.toString)

    queryPart match {
      case singleQuery: SingleQuery =>
        val builder = toPlannerQueryBuilder(singleQuery, semanticTable)
        builder.build()

      case unionQuery: ast.Union =>
        val part: PlannerQueryPart = toPlannerQueryPart(unionQuery.part, semanticTable)
        val query: SinglePlannerQuery = toPlannerQueryBuilder(unionQuery.query, semanticTable).build()


        val distinct = unionQuery match {
          case _: ProjectingUnionAll => false
          case _: ProjectingUnionDistinct => true
        }

        UnionQuery(part, query, distinct, unionQuery.unionMappings)
      case _ =>
        throw new InternalException(s"Received an AST-clause that has no representation the QG: $queryPart")
    }
  }

  /**
    * Flatten consecutive CREATE clauses into one.
    *
    *   CREATE (a) CREATE (b) => CREATE (a),(b)
    */
  def flattenCreates(clauses: Seq[Clause]): Seq[Clause] = {
    val builder = ArrayBuffer.empty[Clause]
    var prevCreate: Option[(Seq[PatternPart], InputPosition)] = None
    for (clause <- clauses) {
      (clause, prevCreate) match {
        case (c: Create, None) =>
          prevCreate = Some((c.pattern.patternParts, c.position))

        case (c: Create, Some((prevParts, pos))) =>
          prevCreate = Some((prevParts ++ c.pattern.patternParts, pos))

        case (nonCreate, Some((prevParts, pos))) =>
          builder += Create(Pattern(prevParts)(pos))(pos)
          builder += nonCreate
          prevCreate = None

        case (nonCreate, None) =>
          builder += nonCreate
      }
    }
    for ((prevParts, pos) <- prevCreate)
      builder += Create(Pattern(prevParts)(pos))(pos)
    builder
  }
}
