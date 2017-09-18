/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_3

import org.neo4j.cypher.internal.compiler.v3_3.phases._
import org.neo4j.cypher.internal.frontend.v3_4._
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.frontend.v3_4.phases.CompilationPhaseTracer.CompilationPhase
import org.neo4j.cypher.internal.frontend.v3_4.phases.CompilationPhaseTracer.CompilationPhase.PIPE_BUILDING
import org.neo4j.cypher.internal.frontend.v3_4.phases.{BaseState, Condition, Phase}
import org.neo4j.cypher.internal.ir.v3_4.IdName
import org.neo4j.cypher.internal.v3_3.logical.plans
import org.neo4j.cypher.internal.v3_3.logical.plans.{LogicalPlan, ResolvedCall}

/**
  * This planner takes on queries that requires no planning such as procedures and schema commands
  */
case object ProcedureCallOrSchemaCommandPlanBuilder extends Phase[CompilerContext, BaseState, LogicalPlanState] {

  override def phase: CompilationPhase = PIPE_BUILDING

  override def description = "take on queries that require no planning such as procedures and schema commands"

  override def postConditions: Set[Condition] = Set.empty

  override def process(from: BaseState, context: CompilerContext): LogicalPlanState = {
    val maybeLogicalPlan: Option[LogicalPlan] = from.statement() match {
      // Global call: CALL foo.bar.baz("arg1", 2)
      case Query(None, SingleQuery(Seq(resolved@ResolvedCall(signature, args, _, _, _)))) =>
        val SemanticCheckResult(_, errors) = resolved.semanticCheck(SemanticState.clean)
        errors.foreach { error => throw context.exceptionCreator(error.msg, error.position) }
        Some(plans.StandAloneProcedureCall(signature, args, resolved.callResultTypes, resolved.callResultIndices))

      // CREATE CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY
      case CreateNodeKeyConstraint(node, label, props) =>
        Some(plans.CreateNodeKeyConstraint(IdName.fromVariable(node), label, props))


      // DROP CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY
      case DropNodeKeyConstraint(_, label, props) =>
        Some(plans.DropNodeKeyConstraint(label, props))

      // CREATE CONSTRAINT ON (node:Label) ASSERT node.prop IS UNIQUE
      // CREATE CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS UNIQUE
      case CreateUniquePropertyConstraint(node, label, props) =>
        Some(plans.CreateUniquePropertyConstraint(IdName.fromVariable(node), label, props))

      // DROP CONSTRAINT ON (node:Label) ASSERT node.prop IS UNIQUE
      // DROP CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS UNIQUE
      case DropUniquePropertyConstraint(_, label, props) =>
        Some(plans.DropUniquePropertyConstraint(label, props))

      // CREATE CONSTRAINT ON (node:Label) ASSERT node.prop EXISTS
      case CreateNodePropertyExistenceConstraint(_, label, prop) =>
        Some(plans.CreateNodePropertyExistenceConstraint(label, prop))

      // DROP CONSTRAINT ON (node:Label) ASSERT node.prop EXISTS
      case DropNodePropertyExistenceConstraint(_, label, prop) =>
        Some(plans.DropNodePropertyExistenceConstraint(label, prop))

      // CREATE CONSTRAINT ON ()-[r:R]-() ASSERT r.prop EXISTS
      case CreateRelationshipPropertyExistenceConstraint(_, relType, prop) =>
        Some(plans.CreateRelationshipPropertyExistenceConstraint(relType, prop))

      // DROP CONSTRAINT ON ()-[r:R]-() ASSERT r.prop EXISTS
      case DropRelationshipPropertyExistenceConstraint(_, relType, prop) =>
        Some(plans.DropRelationshipPropertyExistenceConstraint(relType, prop))

      // CREATE INDEX ON :LABEL(prop)
      case CreateIndex(label, props) =>
        Some(plans.CreateIndex(label, props))

      // DROP INDEX ON :LABEL(prop)
      case DropIndex(label, props) =>
        Some(plans.DropIndex(label, props))

      case _ => None
    }

    LogicalPlanState(from).withMaybeLogicalPlan(maybeLogicalPlan)
  }
}
