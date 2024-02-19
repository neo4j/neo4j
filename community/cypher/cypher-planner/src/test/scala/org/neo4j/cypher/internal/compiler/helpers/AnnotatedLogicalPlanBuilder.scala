/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.helpers

import org.neo4j.cypher.internal.compiler.helpers.AnnotatedLogicalPlanBuilder.AnnotatedPlan
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder
import org.neo4j.cypher.internal.logical.builder.Resolver
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.attribution.Id

object AnnotatedLogicalPlanBuilder {

  case class AnnotatedPlan(plan: LogicalPlan, annotations: Map[String, LogicalPlan]) {
    def get(tag: String): LogicalPlan = annotations(tag)
  }
}

class AnnotatedLogicalPlanBuilder(wholePlan: Boolean = true, resolver: Resolver = new LogicalPlanResolver)
    extends AbstractLogicalPlanBuilder[AnnotatedPlan, AnnotatedLogicalPlanBuilder](resolver, wholePlan) {

  private val annotations = Map.newBuilder[String, Id]

  def fakeLeafPlan(args: String*): AnnotatedLogicalPlanBuilder =
    appendAtCurrentIndent(LeafOperator(FakeLeafPlan(args.toSet)(_)))

  def annotate(tag: String): AnnotatedLogicalPlanBuilder = {
    annotations.addOne(tag -> idOfLastPlan)
    this
  }

  private def planById(root: LogicalPlan): Map[Id, LogicalPlan] = {
    root.folder.treeFold(Map.empty[Id, LogicalPlan]) {
      case plan: LogicalPlan => acc => TraverseChildren(acc.updated(plan.id, plan))
      case _                 => acc => SkipChildren(acc)
    }
  }

  def build(readOnly: Boolean = true): AnnotatedPlan = {
    val root = buildLogicalPlan()
    val idToPlan = planById(root)
    AnnotatedPlan(root, annotations.result().view.mapValues(idToPlan).toMap)
  }
}
