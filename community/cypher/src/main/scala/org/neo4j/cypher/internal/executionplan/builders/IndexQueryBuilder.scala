/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.executionplan.builders

import org.neo4j.cypher.internal.commands._
import org.neo4j.cypher.internal.pipes._
import org.neo4j.graphdb.{Relationship, Node}
import org.neo4j.cypher.internal.executionplan.{PlanBuilder, ExecutionPlanInProgress}
import org.neo4j.cypher.internal.spi.PlanContext

class IndexQueryBuilder extends PlanBuilder {
  def apply(plan: ExecutionPlanInProgress, context: PlanContext) = {
    val q = plan.query
    val p = plan.pipe

    val item = q.start.filter(mapQueryToken(context).isDefinedAt).head

    val newPipe = mapQueryToken(context)(item)(p)

    plan.copy(pipe = newPipe, query = q.copy(start = q.start.filterNot(_ == item) :+ item.solve))
  }

  override def missingDependencies(plan: ExecutionPlanInProgress): Seq[String] = super.missingDependencies(plan)

  def canWorkWith(plan: ExecutionPlanInProgress, context: PlanContext) =
    plan.query.start.exists(mapQueryToken(context).isDefinedAt)

  private def genNodeStart(entityFactory: EntityProducerFactory): PartialFunction[StartItem, EntityProducer[Node]] =
    entityFactory.nodeByIndex orElse
      entityFactory.nodeByIndexQuery orElse
      entityFactory.nodeByIndexHint

  private def genRelationshipStart(entityFactory: EntityProducerFactory): PartialFunction[StartItem, EntityProducer[Relationship]] =
    entityFactory.relationshipByIndex orElse
      entityFactory.relationshipByIndexQuery

  private def mapQueryToken(planContext: PlanContext): PartialFunction[QueryToken[StartItem], (Pipe => Pipe)] = {
    lazy val entityFactory = new EntityProducerFactory(planContext)
    lazy val nodeStart: PartialFunction[StartItem, EntityProducer[Node]] = genNodeStart(entityFactory)
    lazy val relationshipStart: PartialFunction[StartItem, EntityProducer[Relationship]] = genRelationshipStart(entityFactory)
    val result: PartialFunction[QueryToken[StartItem], (Pipe => Pipe)] = {
      case Unsolved(item) if nodeStart.isDefinedAt(item) =>
        (p: Pipe) => new NodeStartPipe(p, item.identifierName, nodeStart.apply(item))

      case Unsolved(item) if relationshipStart.isDefinedAt(item) =>
        (p: Pipe) => new RelationshipStartPipe(p, item.identifierName, relationshipStart.apply(item))
    }
    result
  }


  def priority: Int = PlanBuilder.IndexQuery
}