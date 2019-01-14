/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compatibility

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.executionplan.ExecutionPlan
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.executionplan.procs.{ProcedureCallExecutionPlan, SchemaWriteExecutionPlan}
import org.neo4j.cypher.internal.compiler.v3_5.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.v3_5.planner.CantCompileQueryException
import org.neo4j.cypher.internal.planner.v3_5.spi.IndexDescriptor
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.{InternalQueryType, ProcedureCallMode, QueryContext, SCHEMA_WRITE}
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.neo4j.cypher.internal.v3_5.expressions.{LabelName, PropertyKeyName, RelTypeName}
import org.neo4j.cypher.internal.v3_5.util.{LabelId, PropertyKeyId}

/**
  * This runtime takes on queries that require no planning, such as procedures and schema commands
  */
object ProcedureCallOrSchemaCommandRuntime extends CypherRuntime[RuntimeContext] {
  override def compileToExecutable(state: LogicalPlanState, context: RuntimeContext): ExecutionPlan = {

    def throwCantCompile(unknownPlan: LogicalPlan): Nothing = {
      throw new CantCompileQueryException(
        s"Plan is not a procedure call or schema command: ${unknownPlan.getClass.getSimpleName}")
    }

    logicalToExecutable.applyOrElse(state.maybeLogicalPlan.get, throwCantCompile).apply(context)
  }

  def queryType(logicalPlan: LogicalPlan): Option[InternalQueryType] =
    if (logicalToExecutable.isDefinedAt(logicalPlan)) {
      logicalPlan match {
        case StandAloneProcedureCall(signature, args, types, indices) =>
          Some(ProcedureCallMode.fromAccessMode(signature.accessMode).queryType)
        case _ => Some(SCHEMA_WRITE)
      }
    } else None

  val logicalToExecutable: PartialFunction[LogicalPlan, RuntimeContext => ExecutionPlan] = {
    // Global call: CALL foo.bar.baz("arg1", 2)
    case plan@StandAloneProcedureCall(signature, args, types, indices) => runtimeContext =>
      ProcedureCallExecutionPlan(signature, args, types, indices, new ExpressionConverters(CommunityExpressionConverter(runtimeContext.tokenContext)), plan.id)

    // CREATE CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY
    case CreateNodeKeyConstraint(_, label, props) => runtimeContext =>
      SchemaWriteExecutionPlan("CreateNodeKeyConstraint", ctx => {
              val propertyKeyIds = props.map(p => propertyToId(ctx)(p.propertyKey))
              ctx.createNodeKeyConstraint(IndexDescriptor(labelToId(ctx)(label), propertyKeyIds))
            })

    // DROP CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY
    case DropNodeKeyConstraint(label, props) => runtimeContext =>
      SchemaWriteExecutionPlan("DropNodeKeyConstraint", ctx => {
              val propertyKeyIds = props.map(p => propertyToId(ctx)(p.propertyKey))
              ctx.dropNodeKeyConstraint(IndexDescriptor(labelToId(ctx)(label), propertyKeyIds))
            })

    // CREATE CONSTRAINT ON (node:Label) ASSERT node.prop IS UNIQUE
    // CREATE CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS UNIQUE
    case CreateUniquePropertyConstraint(_, label, props) => runtimeContext =>
      SchemaWriteExecutionPlan("CreateUniqueConstraint", ctx => {
              val propertyKeyIds = props.map(p => propertyToId(ctx)(p.propertyKey))
              ctx.createUniqueConstraint(IndexDescriptor(labelToId(ctx)(label), propertyKeyIds))
            })

    // DROP CONSTRAINT ON (node:Label) ASSERT node.prop IS UNIQUE
    // DROP CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS UNIQUE
    case DropUniquePropertyConstraint(label, props) => runtimeContext =>
      SchemaWriteExecutionPlan("DropUniqueConstraint", ctx => {
              val propertyKeyIds = props.map(p => propertyToId(ctx)(p.propertyKey))
              ctx.dropUniqueConstraint(IndexDescriptor(labelToId(ctx)(label), propertyKeyIds))
            })

    // CREATE CONSTRAINT ON (node:Label) ASSERT node.prop EXISTS
    case CreateNodePropertyExistenceConstraint(label, prop) => runtimeContext =>
      SchemaWriteExecutionPlan("CreateNodePropertyExistenceConstraint", ctx => {
              (ctx.createNodePropertyExistenceConstraint _).tupled(labelProp(ctx)(label, prop.propertyKey))
            })

    // DROP CONSTRAINT ON (node:Label) ASSERT node.prop EXISTS
    case DropNodePropertyExistenceConstraint(label, prop) => runtimeContext =>
      SchemaWriteExecutionPlan("DropNodePropertyExistenceConstraint", ctx => {
              (ctx.dropNodePropertyExistenceConstraint _).tupled(labelProp(ctx)(label, prop.propertyKey))
            })

    // CREATE CONSTRAINT ON ()-[r:R]-() ASSERT r.prop EXISTS
    case CreateRelationshipPropertyExistenceConstraint(relType, prop) => runtimeContext =>
      SchemaWriteExecutionPlan("CreateRelationshipPropertyExistenceConstraint", ctx => {
              (ctx.createRelationshipPropertyExistenceConstraint _).tupled(typeProp(ctx)(relType, prop.propertyKey))
            })

    // DROP CONSTRAINT ON ()-[r:R]-() ASSERT r.prop EXISTS
    case DropRelationshipPropertyExistenceConstraint(relType, prop) => runtimeContext =>
      SchemaWriteExecutionPlan("DropRelationshipPropertyExistenceConstraint", ctx => {
              (ctx.dropRelationshipPropertyExistenceConstraint _).tupled(typeProp(ctx)(relType, prop.propertyKey))
            })

    // CREATE INDEX ON :LABEL(prop)
    case CreateIndex(label, props) => runtimeContext =>
      SchemaWriteExecutionPlan("CreateIndex", ctx => {
              ctx.addIndexRule(IndexDescriptor(labelToId(ctx)(label), propertiesToIds(ctx)(props)))
            })

    // DROP INDEX ON :LABEL(prop)
    case DropIndex(label, props) => runtimeContext =>
      SchemaWriteExecutionPlan("DropIndex", ctx => {
              ctx.dropIndexRule(IndexDescriptor(labelToId(ctx)(label), propertiesToIds(ctx)(props)))
            })
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
