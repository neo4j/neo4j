/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.cypher.internal.ast.CreateIndex
import org.neo4j.cypher.internal.ast.CreateIndexOldSyntax
import org.neo4j.cypher.internal.ast.CreateNodeKeyConstraint
import org.neo4j.cypher.internal.ast.CreateNodePropertyExistenceConstraint
import org.neo4j.cypher.internal.ast.CreateRelationshipPropertyExistenceConstraint
import org.neo4j.cypher.internal.ast.CreateUniquePropertyConstraint
import org.neo4j.cypher.internal.ast.DropConstraintOnName
import org.neo4j.cypher.internal.ast.DropIndex
import org.neo4j.cypher.internal.ast.DropIndexOnName
import org.neo4j.cypher.internal.ast.DropNodeKeyConstraint
import org.neo4j.cypher.internal.ast.DropNodePropertyExistenceConstraint
import org.neo4j.cypher.internal.ast.DropRelationshipPropertyExistenceConstraint
import org.neo4j.cypher.internal.ast.DropUniquePropertyConstraint
import org.neo4j.cypher.internal.ast.IfExistsDoNothing
import org.neo4j.cypher.internal.ast.ShowConstraints
import org.neo4j.cypher.internal.ast.ShowIndexes
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.PIPE_BUILDING
import org.neo4j.cypher.internal.frontend.phases.Condition
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.AdministrationPlannerName
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen

/**
 * This planner takes on queries that requires no planning such as schema commands
 */
case object SchemaCommandPlanBuilder extends Phase[PlannerContext, BaseState, LogicalPlanState] {

  override def phase: CompilationPhase = PIPE_BUILDING

  override def description = "take on queries that require no planning such as schema commands"

  override def postConditions: Set[Condition] = Set.empty

  override def process(from: BaseState, context: PlannerContext): LogicalPlanState = {
    implicit val idGen: SequentialIdGen = new SequentialIdGen()
    val maybeLogicalPlan: Option[LogicalPlan] = from.statement() match {
      // CREATE CONSTRAINT [name] [IF NOT EXISTS] ON (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY [OPTIONS {...}]
      case CreateNodeKeyConstraint(node, label, props, name, ifExistsDo, options, _) =>
        val source = ifExistsDo match {
          case IfExistsDoNothing => Some(plans.DoNothingIfExistsForConstraint(node.name, scala.util.Left(label), props, plans.NodeKey, name))
          case _ => None
        }
        Some(plans.CreateNodeKeyConstraint(source, node.name, label, props, name, options))

      // DROP CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY
      case DropNodeKeyConstraint(_, label, props, _) =>
        Some(plans.DropNodeKeyConstraint(label, props))

      // CREATE CONSTRAINT [name] [IF NOT EXISTS] ON (node:Label) ASSERT node.prop IS UNIQUE [OPTIONS {...}]
      // CREATE CONSTRAINT [name] [IF NOT EXISTS] ON (node:Label) ASSERT (node.prop1,node.prop2) IS UNIQUE [OPTIONS {...}]
      case CreateUniquePropertyConstraint(node, label, props, name, ifExistsDo, options, _) =>
        val source = ifExistsDo match {
          case IfExistsDoNothing => Some(plans.DoNothingIfExistsForConstraint(node.name, scala.util.Left(label), props, plans.Uniqueness, name))
          case _ => None
        }
        Some(plans.CreateUniquePropertyConstraint(source, node.name, label, props, name, options))

      // DROP CONSTRAINT ON (node:Label) ASSERT node.prop IS UNIQUE
      // DROP CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS UNIQUE
      case DropUniquePropertyConstraint(_, label, props, _) =>
        Some(plans.DropUniquePropertyConstraint(label, props))

      // CREATE CONSTRAINT [name] [IF NOT EXISTS] ON (node:Label) ASSERT EXISTS node.prop
      case CreateNodePropertyExistenceConstraint(_, label, prop, name, ifExistsDo, _, _) =>
        val source = ifExistsDo match {
          case IfExistsDoNothing => Some(plans.DoNothingIfExistsForConstraint(prop.map.asCanonicalStringVal, scala.util.Left(label), Seq(prop), plans.NodePropertyExistence, name))
          case _ => None
        }
        Some(plans.CreateNodePropertyExistenceConstraint(source, label, prop, name))

      // DROP CONSTRAINT ON (node:Label) ASSERT EXISTS node.prop
      case DropNodePropertyExistenceConstraint(_, label, prop, _) =>
        Some(plans.DropNodePropertyExistenceConstraint(label, prop))

      // CREATE CONSTRAINT [name] [IF NOT EXISTS] ON ()-[r:R]-() ASSERT EXISTS r.prop
      case CreateRelationshipPropertyExistenceConstraint(_, relType, prop, name, ifExistsDo, _, _) =>
        val source = ifExistsDo match {
          case IfExistsDoNothing => Some(plans.DoNothingIfExistsForConstraint(prop.map.asCanonicalStringVal, scala.util.Right(relType), Seq(prop), plans.RelationshipPropertyExistence, name))
          case _ => None
        }
        Some(plans.CreateRelationshipPropertyExistenceConstraint(source, relType, prop, name))

      // DROP CONSTRAINT ON ()-[r:R]-() ASSERT EXISTS r.prop
      case DropRelationshipPropertyExistenceConstraint(_, relType, prop, _) =>
        Some(plans.DropRelationshipPropertyExistenceConstraint(relType, prop))

      // DROP CONSTRAINT name [IF EXISTS]
      case DropConstraintOnName(name, ifExists, _) =>
        Some(plans.DropConstraintOnName(name, ifExists))

      // SHOW [ALL|UNIQUE|NODE EXIST[S]|RELATIONSHIP EXIST[S]|EXIST[S]|NODE KEY] CONSTRAINT[S] [BRIEF|VERBOSE[OUTPUT]]
      case sc@ShowConstraints(constraintType, verbose, _) =>
        Some(plans.ShowConstraints(constraintType, verbose, sc.defaultColumnNames))
        
      // CREATE INDEX ON :LABEL(prop)
      case CreateIndexOldSyntax(label, props, _) =>
        Some(plans.CreateIndex(None, label, props, None, Map.empty))

      // CREATE INDEX [name] [IF NOT EXISTS] FOR (n:LABEL) ON (n.prop) [OPTIONS {...}]
      case CreateIndex(_, label, props, name, ifExistsDo, options, _) =>
        val propKeys = props.map(_.propertyKey)
        val source = ifExistsDo match {
          case IfExistsDoNothing => Some(plans.DoNothingIfExistsForIndex(label, propKeys, name))
          case _ => None
        }
        Some(plans.CreateIndex(source, label, propKeys, name, options))

      // DROP INDEX ON :LABEL(prop)
      case DropIndex(label, props, _) =>
        Some(plans.DropIndex(label, props))

      // DROP INDEX name [IF EXISTS]
      case DropIndexOnName(name, ifExists, _) =>
        Some(plans.DropIndexOnName(name, ifExists))

      // SHOW [ALL|BTREE] INDEX[ES] [BRIEF|VERBOSE[OUTPUT]]
      case si@ShowIndexes(all, verbose, _) =>
        Some(plans.ShowIndexes(all, verbose, si.defaultColumnNames))

      case _ => None
    }

    val planState = LogicalPlanState(from)

    if (maybeLogicalPlan.isDefined)
      planState.copy(maybeLogicalPlan = maybeLogicalPlan, plannerName = AdministrationPlannerName)
    else planState
  }
}
