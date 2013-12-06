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
package org.neo4j.cypher.internal.compiler.v2_0.executionplan.builders

import org.neo4j.cypher.internal.compiler.v2_0.commands._
import org.neo4j.cypher.internal.compiler.v2_0.pipes._
import org.neo4j.graphdb.{Relationship, Node}
import org.neo4j.cypher.internal.compiler.v2_0.executionplan.{PlanBuilder, ExecutionPlanInProgress}
import org.neo4j.cypher.internal.compiler.v2_0.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v2_0.symbols.SymbolTable

/*
This class is responsible for taking unsolved StartItems and transforming them into StartPipes
 */
class StartPointBuilder extends PlanBuilder {
  def apply(plan: ExecutionPlanInProgress, context: PlanContext) = {
    val q = plan.query
    val p = plan.pipe

    val item = q.start.filter({ (startItemToken: QueryToken[StartItem]) =>
        mapQueryToken().isDefinedAt((context, startItemToken, p.symbols))
      }).head


    val newPipe = mapQueryToken().apply((context, item, p.symbols))(p)

    plan.copy(pipe = newPipe, query = q.copy(start = q.start.filterNot(_ == item) :+ item.solve))
  }

  override def missingDependencies(plan: ExecutionPlanInProgress): Seq[String] = super.missingDependencies(plan)

  def canWorkWith(plan: ExecutionPlanInProgress, context: PlanContext) =
    plan.query.start.exists({ (itemToken: QueryToken[StartItem]) =>
      mapQueryToken().isDefinedAt((context, itemToken, plan.pipe.symbols))
    })

  private def genNodeStart(entityFactory: EntityProducerFactory): MaybeProducerOf[Node] =
    entityFactory.nodeByIndex orElse
      entityFactory.nodeByIndexQuery orElse
      entityFactory.nodeByIndexHint orElse
      entityFactory.nodeById orElse
      entityFactory.nodesAll orElse
      entityFactory.nodeByLabel


  private def genRelationshipStart(entityFactory: EntityProducerFactory): MaybeProducerOf[Relationship] =
    entityFactory.relationshipByIndex orElse
      entityFactory.relationshipByIndexQuery orElse
      entityFactory.relationshipById orElse
      entityFactory.relationshipsAll

  type MaybeProducerOfPipeProducer = PartialFunction[(PlanContext, QueryToken[StartItem], SymbolTable), (Pipe => Pipe)]

  private def mapQueryToken(): MaybeProducerOfPipeProducer = {
    val entityFactory = new EntityProducerFactory
    val nodeStart = genNodeStart(entityFactory)
    val relationshipStart = genRelationshipStart(entityFactory)
    val result: MaybeProducerOfPipeProducer = {
      case (planContext, Unsolved(item), symbols) if nodeStart.isDefinedAt((planContext, item, Some(symbols))) =>
        (p: Pipe) =>
          new NodeStartPipe(p, item.identifierName, nodeStart.apply((planContext, item, Some(symbols))))

      case (planContext, Unsolved(item), symbols) if relationshipStart.isDefinedAt((planContext, item, Some(symbols))) =>
        (p: Pipe) =>
          new RelationshipStartPipe(p, item.identifierName, relationshipStart.apply((planContext, item, Some(symbols))))
    }
    result
  }
}
