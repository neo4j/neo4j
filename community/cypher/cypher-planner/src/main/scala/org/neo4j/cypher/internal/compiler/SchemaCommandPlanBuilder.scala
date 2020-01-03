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
package org.neo4j.cypher.internal.compiler

import org.neo4j.cypher.internal.compiler.phases.{LogicalPlanState, PlannerContext}
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.AdministrationPlannerName
import org.neo4j.cypher.internal.v4_0.ast._
import org.neo4j.cypher.internal.v4_0.frontend.phases.CompilationPhaseTracer.CompilationPhase
import org.neo4j.cypher.internal.v4_0.frontend.phases.CompilationPhaseTracer.CompilationPhase.PIPE_BUILDING
import org.neo4j.cypher.internal.v4_0.frontend.phases.{BaseState, Condition, Phase}
import org.neo4j.cypher.internal.v4_0.util.attribution.SequentialIdGen

/**
  * This planner takes on queries that requires no planning such as schema commands
  */
case object SchemaCommandPlanBuilder extends Phase[PlannerContext, BaseState, LogicalPlanState] {

  override def phase: CompilationPhase = PIPE_BUILDING

  override def description = "take on queries that require no planning such as schema commands"

  override def postConditions: Set[Condition] = Set.empty

  override def process(from: BaseState, context: PlannerContext): LogicalPlanState = {
    implicit val idGen = new SequentialIdGen()
    val maybeLogicalPlan: Option[LogicalPlan] = from.statement() match {
      // CREATE CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY
      case CreateNodeKeyConstraint(node, label, props, name) =>
        Some(plans.CreateNodeKeyConstraint(node.name, label, props, name))

      // DROP CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY
      case DropNodeKeyConstraint(_, label, props) =>
        Some(plans.DropNodeKeyConstraint(label, props))

      // CREATE CONSTRAINT ON (node:Label) ASSERT node.prop IS UNIQUE
      // CREATE CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS UNIQUE
      case CreateUniquePropertyConstraint(node, label, props, name) =>
        Some(plans.CreateUniquePropertyConstraint(node.name, label, props, name))

      // DROP CONSTRAINT ON (node:Label) ASSERT node.prop IS UNIQUE
      // DROP CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS UNIQUE
      case DropUniquePropertyConstraint(_, label, props) =>
        Some(plans.DropUniquePropertyConstraint(label, props))

      // CREATE CONSTRAINT ON (node:Label) ASSERT node.prop EXISTS
      case CreateNodePropertyExistenceConstraint(_, label, prop, name) =>
        Some(plans.CreateNodePropertyExistenceConstraint(label, prop, name))

      // DROP CONSTRAINT ON (node:Label) ASSERT node.prop EXISTS
      case DropNodePropertyExistenceConstraint(_, label, prop) =>
        Some(plans.DropNodePropertyExistenceConstraint(label, prop))

      // CREATE CONSTRAINT ON ()-[r:R]-() ASSERT r.prop EXISTS
      case CreateRelationshipPropertyExistenceConstraint(_, relType, prop, name) =>
        Some(plans.CreateRelationshipPropertyExistenceConstraint(relType, prop, name))

      // DROP CONSTRAINT ON ()-[r:R]-() ASSERT r.prop EXISTS
      case DropRelationshipPropertyExistenceConstraint(_, relType, prop) =>
        Some(plans.DropRelationshipPropertyExistenceConstraint(relType, prop))

      // DROP CONSTRAINT name
      case DropConstraintOnName(name) =>
        Some(plans.DropConstraintOnName(name))

      // CREATE INDEX ON :LABEL(prop)
      case CreateIndex(label, props) =>
        Some(plans.CreateIndex(label, props, None))

      // CREATE INDEX FOR (n:LABEL) ON (n.prop)
      // CREATE INDEX name FOR (n:LABEL) ON (n.prop)
      case CreateIndexNewSyntax(_, label, props, name) =>
        Some(plans.CreateIndex(label, props.map(_.propertyKey), name))

      // DROP INDEX ON :LABEL(prop)
      case DropIndex(label, props) =>
        Some(plans.DropIndex(label, props))

      // DROP INDEX name
      case DropIndexOnName(name) =>
        Some(plans.DropIndexOnName(name))

      case _ => None
    }

    val planState = LogicalPlanState(from)

    if (maybeLogicalPlan.isDefined)
      planState.copy(maybeLogicalPlan = maybeLogicalPlan, plannerName = AdministrationPlannerName)
    else planState
  }
}
