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
package org.neo4j.cypher.internal.compiler

import org.neo4j.common.EntityType
import org.neo4j.cypher.internal.ast.CreateConstraint
import org.neo4j.cypher.internal.ast.CreateFulltextIndex
import org.neo4j.cypher.internal.ast.CreateLookupIndex
import org.neo4j.cypher.internal.ast.CreateSingleLabelPropertyIndex
import org.neo4j.cypher.internal.ast.DropConstraintOnName
import org.neo4j.cypher.internal.ast.DropIndexOnName
import org.neo4j.cypher.internal.ast.IfExistsDoNothing
import org.neo4j.cypher.internal.ast.PointCreateIndex
import org.neo4j.cypher.internal.ast.RangeCreateIndex
import org.neo4j.cypher.internal.ast.TextCreateIndex
import org.neo4j.cypher.internal.ast.VectorCreateIndex
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.PIPE_BUILDING
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.AdministrationPlannerName
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen
import org.neo4j.graphdb.schema.IndexType

/**
 * This planner takes on queries that requires no planning such as schema commands
 */
case object SchemaCommandPlanBuilder extends Phase[PlannerContext, BaseState, LogicalPlanState] {

  override def phase: CompilationPhase = PIPE_BUILDING

  override def postConditions: Set[StepSequencer.Condition] = Set.empty

  override def process(from: BaseState, context: PlannerContext): LogicalPlanState = {
    implicit val idGen: SequentialIdGen = new SequentialIdGen()

    val maybeLogicalPlan: Option[LogicalPlan] = from.statement() match {
      // CREATE CONSTRAINT ... IS KEY
      // CREATE CONSTRAINT ... IS UNIQUE
      // CREATE CONSTRAINT ... IS NOT NULL
      // CREATE CONSTRAINT ... IS :: ...
      case CreateConstraint(_, entityName, props, name, constraintType, ifExistsDo, options) =>
        val source = ifExistsDo match {
          case IfExistsDoNothing => Some(plans.DoNothingIfExistsForConstraint(
              entityName,
              props,
              constraintType,
              name,
              options
            ))
          case _ => None
        }
        Some(plans.CreateConstraint(source, constraintType, entityName, props, name, options))

      // DROP CONSTRAINT name [IF EXISTS]
      case DropConstraintOnName(name, ifExists, _) =>
        Some(plans.DropConstraintOnName(name, ifExists))

      // CREATE [POINT| RANGE | TEXT | VECTOR] INDEX ...
      case CreateSingleLabelPropertyIndex(_, entityName, props, name, astIndexType, ifExistsDo, options) =>
        val indexType = astIndexType match {
          case PointCreateIndex    => IndexType.POINT
          case _: RangeCreateIndex => IndexType.RANGE
          case TextCreateIndex     => IndexType.TEXT
          case VectorCreateIndex   => IndexType.VECTOR
          case it =>
            throw new IllegalStateException(
              s"Did not expect index type ${it.command} here: only point, range, text or vector indexes."
            )
        }
        val propKeys = props.map(_.propertyKey)
        val source = ifExistsDo match {
          case IfExistsDoNothing =>
            Some(plans.DoNothingIfExistsForIndex(entityName, propKeys, indexType, name, options))
          case _ => None
        }
        Some(plans.CreateIndex(source, indexType, entityName, propKeys, name, options))

      // CREATE LOOKUP INDEX ...
      case CreateLookupIndex(_, isNodeIndex, _, name, _, ifExistsDo, options) =>
        val entityType = if (isNodeIndex) EntityType.NODE else EntityType.RELATIONSHIP
        val source = ifExistsDo match {
          case IfExistsDoNothing =>
            Some(plans.DoNothingIfExistsForLookupIndex(entityType, name, options))
          case _ => None
        }
        Some(plans.CreateLookupIndex(source, entityType, name, options))

      // CREATE FULLTEXT INDEX ...
      case CreateFulltextIndex(_, entityNames, props, name, _, ifExistsDo, options) =>
        val propKeys = props.map(_.propertyKey)
        val source = ifExistsDo match {
          case IfExistsDoNothing =>
            Some(plans.DoNothingIfExistsForFulltextIndex(entityNames, propKeys, name, options))
          case _ => None
        }
        Some(plans.CreateFulltextIndex(source, entityNames, propKeys, name, options))

      // DROP INDEX name [IF EXISTS]
      case DropIndexOnName(name, ifExists, _) =>
        Some(plans.DropIndexOnName(name, ifExists))

      case _ => None
    }

    val planState = LogicalPlanState(from)

    if (maybeLogicalPlan.isDefined)
      planState.copy(maybeLogicalPlan = maybeLogicalPlan, plannerName = AdministrationPlannerName)
    else planState
  }
}
