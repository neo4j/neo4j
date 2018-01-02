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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan.builders

import org.neo4j.cypher.internal.compiler.v2_3.commands._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.Identifier
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.Equals
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{ExecutionPlanInProgress, PlanBuilder}
import org.neo4j.cypher.internal.compiler.v2_3.pipes._
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext
import org.neo4j.graphdb.{Node, Relationship}

/*
This class is responsible for taking unsolved StartItems and transforming them into StartPipes
 */
class StartPointBuilder extends PlanBuilder {
  def apply(plan: ExecutionPlanInProgress, context: PlanContext)(implicit pipeMonitor: PipeMonitor) = {
    val q = plan.query
    val p = plan.pipe

    val item = q.start.filter({ (startItemToken: QueryToken[StartItem]) =>
        mapQueryToken().isDefinedAt((context, startItemToken))
      }).head


    val newPipe = mapQueryToken().apply((context, item))(p)

    plan.copy(pipe = newPipe, query = q.copy(start = q.start.filterNot(_ == item) :+ item.solve))
  }

  override def missingDependencies(plan: ExecutionPlanInProgress): Seq[String] = super.missingDependencies(plan)

  def canWorkWith(plan: ExecutionPlanInProgress, context: PlanContext)(implicit pipeMonitor: PipeMonitor) =
    plan.query.start.exists({ (itemToken: QueryToken[StartItem]) =>
      mapQueryToken().isDefinedAt((context, itemToken))
    })

  private def genNodeStart(entityFactory: EntityProducerFactory): PartialFunction[(PlanContext, StartItem), EntityProducer[Node]] =
    entityFactory.nodeByIndex orElse
      entityFactory.nodeByIndexQuery orElse
      entityFactory.nodeByIndexHint(readOnly = true) orElse
      entityFactory.nodeById orElse
      entityFactory.nodesAll orElse
      entityFactory.nodeByLabel


  private def genRelationshipStart(entityFactory: EntityProducerFactory): PartialFunction[(PlanContext, StartItem), EntityProducer[Relationship]] =
    entityFactory.relationshipByIndex orElse
      entityFactory.relationshipByIndexQuery orElse
      entityFactory.relationshipById orElse
      entityFactory.relationshipsAll

  private def mapQueryToken()(implicit pipeMonitor: PipeMonitor): PartialFunction[(PlanContext, QueryToken[StartItem]), (Pipe => Pipe)] = {
    val entityFactory = new EntityProducerFactory
    val nodeStart = genNodeStart(entityFactory)
    val relationshipStart = genRelationshipStart(entityFactory)
    val result: PartialFunction[(PlanContext, QueryToken[StartItem]), (Pipe => Pipe)] = {
      case (planContext, Unsolved(item)) if nodeStart.isDefinedAt((planContext, item)) =>
        (p: Pipe) =>
          new NodeStartPipe(p, item.identifierName, nodeStart.apply((planContext, item)), item.effects)()

      case (planContext, Unsolved(item)) if relationshipStart.isDefinedAt((planContext, item)) => {
        case (p: Pipe) if p.symbols.hasIdentifierNamed(item.identifierName) =>
          val compKey: String = s"  --rel-${item.identifierName}--"
          val relationshipByIndex = new RelationshipStartPipe(p, compKey, relationshipStart.apply((planContext, item)))()
          val relEqualPred = Equals(Identifier(item.identifierName), Identifier(compKey))
          new FilterPipe(relationshipByIndex, relEqualPred)()
        case (p: Pipe) => new RelationshipStartPipe(p, item.identifierName, relationshipStart.apply((planContext, item)))()
      }
    }
    result
  }
}
