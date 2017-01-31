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
package org.neo4j.cypher.internal.compiler.v3_2.executionplan.procs

import org.neo4j.cypher.internal.compiler.v3_2.CompilationPhaseTracer.CompilationPhase
import org.neo4j.cypher.internal.compiler.v3_2.CompilationPhaseTracer.CompilationPhase.PIPE_BUILDING
import org.neo4j.cypher.internal.compiler.v3_2.IndexDescriptor
import org.neo4j.cypher.internal.compiler.v3_2.ast.ResolvedCall
import org.neo4j.cypher.internal.compiler.v3_2.executionplan._
import org.neo4j.cypher.internal.compiler.v3_2.phases.{CompilationState, Condition, Context, Phase}
import org.neo4j.cypher.internal.compiler.v3_2.spi.QueryContext
import org.neo4j.cypher.internal.frontend.v3_2._
import org.neo4j.cypher.internal.frontend.v3_2.ast._

/**
  * This planner takes on queries that requires no planning such as procedures and schema commands
  */
case object ProcedureCallOrSchemaCommandPlanBuilder extends Phase[Context] {

  override def phase: CompilationPhase = PIPE_BUILDING

  override def description = "take on queries that require no planning such as procedures and schema commands"

  override def postConditions: Set[Condition] = Set.empty

  override def process(from: CompilationState, context: Context): CompilationState = {
    val maybeExecutionPlan: Option[ExecutionPlan] = from.statement match {
      // Global call: CALL foo.bar.baz("arg1", 2)
      case Query(None, SingleQuery(Seq(resolved@ResolvedCall(signature, args, _, _, _)))) =>
        val SemanticCheckResult(_, errors) = resolved.semanticCheck(SemanticState.clean)
        errors.foreach { error => throw context.exceptionCreator(error.msg, error.position) }

        Some(ProcedureCallExecutionPlan(signature, args, resolved.callResultTypes, resolved.callResultIndices,
          context.notificationLogger.notifications, context.typeConverter.asPublicType))

      // CREATE CONSTRAINT ON (node:Label) ASSERT node.prop IS UNIQUE
      case CreateUniquePropertyConstraint(node, label, prop) =>
        Some(PureSideEffectExecutionPlan("CreateUniqueConstraint", SCHEMA_WRITE, (ctx) => {
          ctx.createUniqueConstraint(IndexDescriptor(labelToId(ctx)(label), propertyToId(ctx)(prop.propertyKey)))
        }))

      // DROP CONSTRAINT ON (node:Label) ASSERT node.prop IS UNIQUE
      case DropUniquePropertyConstraint(_, label, prop) =>
        Some(PureSideEffectExecutionPlan("DropUniqueConstraint", SCHEMA_WRITE, (ctx) => {
          ctx.dropUniqueConstraint(IndexDescriptor(labelToId(ctx)(label), propertyToId(ctx)(prop.propertyKey)))
        }))

      // CREATE CONSTRAINT ON (node:Label) ASSERT node.prop EXISTS
      case CreateNodePropertyExistenceConstraint(_, label, prop) =>
        Some(PureSideEffectExecutionPlan("CreateNodePropertyExistenceConstraint", SCHEMA_WRITE, (ctx) => {
          (ctx.createNodePropertyExistenceConstraint _).tupled(labelProp(ctx)(label, prop.propertyKey))
        }))

      // DROP CONSTRAINT ON (node:Label) ASSERT node.prop EXISTS
      case DropNodePropertyExistenceConstraint(_, label, prop) =>
        Some(PureSideEffectExecutionPlan("CreateNodePropertyExistenceConstraint", SCHEMA_WRITE, (ctx) => {
          (ctx.dropNodePropertyExistenceConstraint _).tupled(labelProp(ctx)(label, prop.propertyKey))
        }))

      // CREATE CONSTRAINT ON ()-[r:R]-() ASSERT r.prop EXISTS
      case CreateRelationshipPropertyExistenceConstraint(_, relType, prop) =>
        Some(PureSideEffectExecutionPlan("CreateRelationshipPropertyExistenceConstraint", SCHEMA_WRITE, (ctx) => {
          (ctx.createRelationshipPropertyExistenceConstraint _).tupled(typeProp(ctx)(relType, prop.propertyKey))
        }))

      // DROP CONSTRAINT ON ()-[r:R]-() ASSERT r.prop EXISTS
      case DropRelationshipPropertyExistenceConstraint(_, relType, prop) =>
        Some(PureSideEffectExecutionPlan("DropRelationshipPropertyExistenceConstraint", SCHEMA_WRITE, (ctx) => {
          (ctx.dropRelationshipPropertyExistenceConstraint _).tupled(typeProp(ctx)(relType, prop.propertyKey))
        }))

      // CREATE INDEX ON :LABEL(prop)
      case CreateIndex(label, props) =>
        Some(PureSideEffectExecutionPlan("CreateIndex", SCHEMA_WRITE, (ctx) => {
          ctx.addIndexRule(IndexDescriptor(labelToId(ctx)(label), propertiesToIds(ctx)(props)))
        }))

      // DROP INDEX ON :LABEL(prop)
      case DropIndex(label, props) =>
        Some(PureSideEffectExecutionPlan("DropIndex", SCHEMA_WRITE, (ctx) => {
          ctx.dropIndexRule(IndexDescriptor(labelToId(ctx)(label), propertiesToIds(ctx)(props)))
        }))

      case _ => None
    }

    from.copy(maybeExecutionPlan = maybeExecutionPlan)
  }

  implicit private def labelToId(ctx: QueryContext)(label: LabelName): LabelId =
    LabelId(ctx.getOrCreateLabelId(label.name))

  implicit private def propertyToId(ctx: QueryContext)(property: PropertyKeyName): PropertyKeyId =
    PropertyKeyId(ctx.getOrCreatePropertyKeyId(property.name))

  implicit private def propertiesToIds(ctx: QueryContext)(properties: List[PropertyKeyName]): List[PropertyKeyId] =
    properties.map(property => PropertyKeyId(ctx.getOrCreatePropertyKeyId(property.name)))

  private def labelProp(ctx: QueryContext)(label: LabelName, prop: PropertyKeyName) =
    (ctx.getOrCreateLabelId(label.name), ctx.getOrCreatePropertyKeyId(prop.name))

  private def typeProp(ctx: QueryContext)(relType: RelTypeName, prop: PropertyKeyName) =
    (ctx.getOrCreateRelTypeId(relType.name), ctx.getOrCreatePropertyKeyId(prop.name))
}
