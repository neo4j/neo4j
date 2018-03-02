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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan.procs

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.CommunityRuntimeContext
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan.ExecutionPlan
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.helpers.InternalWrapping._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.phases.CompilationState
import org.neo4j.cypher.internal.compiler.v3_4.phases._
import org.neo4j.cypher.internal.frontend.v3_4.phases.CompilationPhaseTracer.CompilationPhase
import org.neo4j.cypher.internal.frontend.v3_4.phases.CompilationPhaseTracer.CompilationPhase.PIPE_BUILDING
import org.neo4j.cypher.internal.frontend.v3_4.phases.{Condition, Phase}
import org.neo4j.cypher.internal.planner.v3_4.spi.IndexDescriptor
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.{QueryContext, SCHEMA_WRITE}
import org.neo4j.cypher.internal.util.v3_4.{LabelId, PropertyKeyId}
import org.neo4j.cypher.internal.v3_4.expressions.{LabelName, PropertyKeyName, RelTypeName}
import org.neo4j.cypher.internal.v3_4.logical.plans._

import scala.util.{Failure, Success, Try}

/**
  * This builder takes on queries that requires no planning such as procedures and schema commands
  */
case object ProcedureCallOrSchemaCommandExecutionPlanBuilder extends Phase[CommunityRuntimeContext, LogicalPlanState, CompilationState] {

  override def phase: CompilationPhase = PIPE_BUILDING

  override def description = "take on queries that require no planning such as procedures and schema commands"

  override def postConditions: Set[Condition] = Set.empty

  override def process(from: LogicalPlanState, context: CommunityRuntimeContext): CompilationState = {
    val maybeExecutionPlan: Try[ExecutionPlan] = from.maybeLogicalPlan match {
      case None => throw new IllegalStateException("A proper logical plan must have been built by now")
      case Some(plan) => plan match {
        // Global call: CALL foo.bar.baz("arg1", 2)
        case StandAloneProcedureCall(signature, args, types, indices) =>
          val converters = new ExpressionConverters(CommunityExpressionConverter)
          val logger = context.notificationLogger
          Success(ProcedureCallExecutionPlan(signature, args, types, indices,
                                          logger.notifications.map(asKernelNotification(logger.offset)), converters))

        // CREATE CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY
        case CreateNodeKeyConstraint(_, label, props) =>
          Success(PureSideEffectExecutionPlan("CreateNodeKeyConstraint", SCHEMA_WRITE, (ctx) => {
            val propertyKeyIds = props.map(p => propertyToId(ctx)(p.propertyKey))
            ctx.createNodeKeyConstraint(IndexDescriptor(labelToId(ctx)(label), propertyKeyIds))
          }))

        // DROP CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY
        case DropNodeKeyConstraint(label, props) =>
          Success(PureSideEffectExecutionPlan("DropNodeKeyConstraint", SCHEMA_WRITE, (ctx) => {
            val propertyKeyIds = props.map(p => propertyToId(ctx)(p.propertyKey))
            ctx.dropNodeKeyConstraint(IndexDescriptor(labelToId(ctx)(label), propertyKeyIds))
          }))

        // CREATE CONSTRAINT ON (node:Label) ASSERT node.prop IS UNIQUE
        // CREATE CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS UNIQUE
        case CreateUniquePropertyConstraint(_, label, props) =>
          Success(PureSideEffectExecutionPlan("CreateUniqueConstraint", SCHEMA_WRITE, (ctx) => {
            val propertyKeyIds = props.map(p => propertyToId(ctx)(p.propertyKey))
            ctx.createUniqueConstraint(IndexDescriptor(labelToId(ctx)(label), propertyKeyIds))
          }))

        // DROP CONSTRAINT ON (node:Label) ASSERT node.prop IS UNIQUE
        // DROP CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS UNIQUE
        case DropUniquePropertyConstraint(label, props) =>
          Success(PureSideEffectExecutionPlan("DropUniqueConstraint", SCHEMA_WRITE, (ctx) => {
            val propertyKeyIds = props.map(p => propertyToId(ctx)(p.propertyKey))
            ctx.dropUniqueConstraint(IndexDescriptor(labelToId(ctx)(label), propertyKeyIds))
          }))

        // CREATE CONSTRAINT ON (node:Label) ASSERT node.prop EXISTS
        case CreateNodePropertyExistenceConstraint(label, prop) =>
          Success(PureSideEffectExecutionPlan("CreateNodePropertyExistenceConstraint", SCHEMA_WRITE, (ctx) => {
            (ctx.createNodePropertyExistenceConstraint _).tupled(labelProp(ctx)(label, prop.propertyKey))
          }))

        // DROP CONSTRAINT ON (node:Label) ASSERT node.prop EXISTS
        case DropNodePropertyExistenceConstraint(label, prop) =>
          Success(PureSideEffectExecutionPlan("DropNodePropertyExistenceConstraint", SCHEMA_WRITE, (ctx) => {
            (ctx.dropNodePropertyExistenceConstraint _).tupled(labelProp(ctx)(label, prop.propertyKey))
          }))

        // CREATE CONSTRAINT ON ()-[r:R]-() ASSERT r.prop EXISTS
        case CreateRelationshipPropertyExistenceConstraint(relType, prop) =>
          Success(PureSideEffectExecutionPlan("CreateRelationshipPropertyExistenceConstraint", SCHEMA_WRITE, (ctx) => {
            (ctx.createRelationshipPropertyExistenceConstraint _).tupled(typeProp(ctx)(relType, prop.propertyKey))
          }))

        // DROP CONSTRAINT ON ()-[r:R]-() ASSERT r.prop EXISTS
        case DropRelationshipPropertyExistenceConstraint(relType, prop) =>
          Success(PureSideEffectExecutionPlan("DropRelationshipPropertyExistenceConstraint", SCHEMA_WRITE, (ctx) => {
            (ctx.dropRelationshipPropertyExistenceConstraint _).tupled(typeProp(ctx)(relType, prop.propertyKey))
          }))

        // CREATE INDEX ON :LABEL(prop)
        case CreateIndex(label, props) =>
          Success(PureSideEffectExecutionPlan("CreateIndex", SCHEMA_WRITE, (ctx) => {
            ctx.addIndexRule(IndexDescriptor(labelToId(ctx)(label), propertiesToIds(ctx)(props)))
          }))

        // DROP INDEX ON :LABEL(prop)
        case DropIndex(label, props) =>
          Success(PureSideEffectExecutionPlan("DropIndex", SCHEMA_WRITE, (ctx) => {
            ctx.dropIndexRule(IndexDescriptor(labelToId(ctx)(label), propertiesToIds(ctx)(props)))
          }))

        case unknownPlan => Failure(new UnsupportedOperationException(
          s"Plan is not a procedure Call or schema command: ${unknownPlan.getClass.getSimpleName}"))
      }
    }

    new CompilationState(from, maybeExecutionPlan)
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
