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

import org.neo4j.cypher.internal.executionplan.{PartiallySolvedQuery, PlanBuilder}
import org.neo4j.cypher.internal.spi.PlanContext
import org.neo4j.cypher.internal.commands._
import org.neo4j.cypher.internal.mutation._
import org.neo4j.cypher.internal.commands.expressions.{Identifier, Property}
import org.neo4j.cypher.internal.commands.values.KeyToken
import org.neo4j.cypher.internal.mutation.UniqueMergeNodeProducers
import org.neo4j.cypher.internal.mutation.MergeNodeAction
import org.neo4j.cypher.internal.executionplan.ExecutionPlanInProgress
import org.neo4j.cypher.internal.commands.SchemaIndex
import org.neo4j.cypher.internal.mutation.PlainMergeNodeProducer
import org.neo4j.cypher.internal.commands.HasLabel
import org.neo4j.cypher.internal.commands.Equals
import org.neo4j.cypher.internal.commands.expressions.Nullable

/*
This builder is concerned with finding queries that use MERGE, and finds a way to try to find matching nodes
 */

class MergeStartPointBuilder extends PlanBuilder {
  val entityProducerFactory = new EntityProducerFactory


  def apply(plan: ExecutionPlanInProgress, ctx: PlanContext): ExecutionPlanInProgress = {

    val q: PartiallySolvedQuery = plan.query

    // Find merge points that do not have a node producer, and produce one for them
    val updatesWithSolvedMergePoints = plan.query.updates.map(update => update.map(solveUnsolvedMergePoints(ctx)))

    plan.copy(query = q.copy(updates = updatesWithSolvedMergePoints))
  }

  private def solveUnsolvedMergePoints(ctx: PlanContext): (UpdateAction => UpdateAction) = {
    case merge: MergeNodeAction if merge.maybeNodeProducer.isEmpty => findNodeProducer(merge, ctx)
    case foreach: ForeachAction                                    => foreach.copy(actions = foreach.actions.map(solveUnsolvedMergePoints(ctx)))
    case x                                                         => x
  }

  private def findNodeProducer(mergeNodeAction: MergeNodeAction, ctx: PlanContext): MergeNodeAction = {
    val identifier = mergeNodeAction.identifier
    val props = mergeNodeAction.props
    val labels = mergeNodeAction.labels
    val where = mergeNodeAction.expectations

    val newMergeNodeAction: MergeNodeAction = NodeFetchStrategy.findUniqueIndexes(props, labels, ctx) match {
      case indexes if indexes.isEmpty =>
        val startItem = NodeFetchStrategy.findStartStrategy(identifier, where, ctx)

        val nodeProducer = PlainMergeNodeProducer(entityProducerFactory.nodeStartItems((ctx, startItem.s)))
        val solvedPredicates = startItem.solvedPredicates
        val predicatesLeft = where.toSet -- solvedPredicates

        mergeNodeAction.copy(maybeNodeProducer = Some(nodeProducer), expectations = predicatesLeft.toSeq)

      case indexes =>
        val startItems: Seq[(KeyToken, KeyToken, RatedStartItem)] = indexes.map {
          case ((label, key)) =>
            val equalsPredicates = Seq(
              Equals(Nullable(Property(Identifier(identifier), key)), props(key)),
              Equals(props(key), Nullable(Property(Identifier(identifier), key)))
            )
            val hasLabelPredicates = labels.map {
              HasLabel(Identifier(identifier), _)
            }
            val predicates = equalsPredicates ++ hasLabelPredicates
            (label, key, RatedStartItem(SchemaIndex(identifier, label.name, key.name, UniqueIndex, Some(props(key))), NodeFetchStrategy.IndexEquality, predicates))
        }

        val nodeProducer = UniqueMergeNodeProducers(startItems.map {
          case (label: KeyToken, propertyKey: KeyToken, item: RatedStartItem) => IndexNodeProducer(label, propertyKey, entityProducerFactory.nodeStartItems((ctx, item.s)))
        })
        val solvedPredicates = startItems.flatMap {
          case (_, _, item: RatedStartItem) => item.solvedPredicates
        }
        val predicatesLeft = where.toSet -- solvedPredicates

        mergeNodeAction.copy(maybeNodeProducer = Some(nodeProducer), expectations = predicatesLeft.toSeq)
    }

    newMergeNodeAction
  }

  def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext) =
    !plan.query.extracted && plan != apply(plan, ctx) // TODO: This can be optimized

  def priority = PlanBuilder.IndexLookup
}
