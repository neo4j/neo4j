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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.procs.SchemaWriteExecutionPlan
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.v4_0.expressions.{LabelName, PropertyKeyName, RelTypeName}
import org.neo4j.cypher.internal.v4_0.util.{LabelId, PropertyKeyId}
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.internal.kernel.api.security.SecurityContext

/**
  * This runtime takes on queries that require no planning such as schema commands
  */
object SchemaCommandRuntime extends CypherRuntime[RuntimeContext] {
  override def name: String = "schema"

  override def compileToExecutable(state: LogicalQuery, context: RuntimeContext, securityContext: SecurityContext): ExecutionPlan = {

    def throwCantCompile(unknownPlan: LogicalPlan): Nothing = {
      throw new CantCompileQueryException(
        s"Plan is not a schema command: ${unknownPlan.getClass.getSimpleName}")
    }
    val (withSlottedParameters, parameterMapping) = slottedParameters(state.logicalPlan)

    logicalToExecutable.applyOrElse(withSlottedParameters, throwCantCompile).apply(context, parameterMapping)
  }

  def queryType(logicalPlan: LogicalPlan): Option[InternalQueryType] =
    if (logicalToExecutable.isDefinedAt(logicalPlan)) {
      Some(SCHEMA_WRITE)
    } else None

  val logicalToExecutable: PartialFunction[LogicalPlan, (RuntimeContext, ParameterMapping) => ExecutionPlan] = {
    // CREATE CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY
    case CreateNodeKeyConstraint(_, label, props, name) => (_, _) =>
      SchemaWriteExecutionPlan("CreateNodeKeyConstraint", ctx => {
              val labelId = ctx.getOrCreateLabelId(label.name)
              val propertyKeyIds = props.map(p => propertyToId(ctx)(p.propertyKey).id)
              ctx.createNodeKeyConstraint(labelId, propertyKeyIds, name)
            })

    // DROP CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY
    case DropNodeKeyConstraint(label, props) => (_, _) =>
      SchemaWriteExecutionPlan("DropNodeKeyConstraint", ctx => {
              val labelId = ctx.getOrCreateLabelId(label.name)
              val propertyKeyIds = props.map(p => propertyToId(ctx)(p.propertyKey).id)
              ctx.dropNodeKeyConstraint(labelId, propertyKeyIds)
            })

    // CREATE CONSTRAINT ON (node:Label) ASSERT node.prop IS UNIQUE
    // CREATE CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS UNIQUE
    case CreateUniquePropertyConstraint(_, label, props, name) => (_, _) =>
      SchemaWriteExecutionPlan("CreateUniqueConstraint", ctx => {
              val labelId = ctx.getOrCreateLabelId(label.name)
              val propertyKeyIds = props.map(p => propertyToId(ctx)(p.propertyKey).id)
              ctx.createUniqueConstraint(labelId, propertyKeyIds, name)
            })

    // DROP CONSTRAINT ON (node:Label) ASSERT node.prop IS UNIQUE
    // DROP CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS UNIQUE
    case DropUniquePropertyConstraint(label, props) => (_, _) =>
      SchemaWriteExecutionPlan("DropUniqueConstraint", ctx => {
              val labelId = ctx.getOrCreateLabelId(label.name)
              val propertyKeyIds = props.map(p => propertyToId(ctx)(p.propertyKey).id)
              ctx.dropUniqueConstraint(labelId, propertyKeyIds)
            })

    // CREATE CONSTRAINT ON (node:Label) ASSERT node.prop EXISTS
    case CreateNodePropertyExistenceConstraint(label, prop, name) => (_, _) =>
      SchemaWriteExecutionPlan("CreateNodePropertyExistenceConstraint", ctx => {
              (ctx.createNodePropertyExistenceConstraint _).tupled(labelPropWithName(ctx)(label, prop.propertyKey, name))
            })

    // DROP CONSTRAINT ON (node:Label) ASSERT node.prop EXISTS
    case DropNodePropertyExistenceConstraint(label, prop) => (_, _) =>
      SchemaWriteExecutionPlan("DropNodePropertyExistenceConstraint", ctx => {
              (ctx.dropNodePropertyExistenceConstraint _).tupled(labelProp(ctx)(label, prop.propertyKey))
            })

    // CREATE CONSTRAINT ON ()-[r:R]-() ASSERT r.prop EXISTS
    case CreateRelationshipPropertyExistenceConstraint(relType, prop, name) => (_, _) =>
      SchemaWriteExecutionPlan("CreateRelationshipPropertyExistenceConstraint", ctx => {
              (ctx.createRelationshipPropertyExistenceConstraint _).tupled(typePropWithName(ctx)(relType, prop.propertyKey, name))
            })

    // DROP CONSTRAINT ON ()-[r:R]-() ASSERT r.prop EXISTS
    case DropRelationshipPropertyExistenceConstraint(relType, prop) => (_, _) =>
      SchemaWriteExecutionPlan("DropRelationshipPropertyExistenceConstraint", ctx => {
              (ctx.dropRelationshipPropertyExistenceConstraint _).tupled(typeProp(ctx)(relType, prop.propertyKey))
            })

    case DropConstraintOnName(name) => (_, _) =>
      SchemaWriteExecutionPlan("DropConstraint", ctx => ctx.dropNamedConstraint(name))

    // CREATE INDEX ON :LABEL(prop)
    // CREATE INDEX FOR (n:LABEL) ON (n.prop)
    // CREATE INDEX name FOR (n:LABEL) ON (n.prop)
    case CreateIndex(label, props, name) => (_, _) =>
      SchemaWriteExecutionPlan("CreateIndex", ctx => {
              val labelId = ctx.getOrCreateLabelId(label.name)
              val propertyKeyIds = props.map(p => propertyToId(ctx)(p).id)
              ctx.addIndexRule(labelId, propertyKeyIds, name)
            })

    // DROP INDEX ON :LABEL(prop)
    case DropIndex(label, props) => (_, _) =>
      SchemaWriteExecutionPlan("DropIndex", ctx => {
              val labelId = labelToId(ctx)(label)
              val propertyKeyIds = props.map(p => propertyToId(ctx)(p).id)
              ctx.dropIndexRule(labelId, propertyKeyIds)
            })

    // DROP INDEX name
    case DropIndexOnName(name) => (_, _) =>
      SchemaWriteExecutionPlan("DropIndex", ctx => ctx.dropIndexRule(name))
  }

  def isApplicable(logicalPlanState: LogicalPlanState): Boolean = logicalToExecutable.isDefinedAt(logicalPlanState.maybeLogicalPlan.get)

  implicit private def labelToId(ctx: QueryContext)(label: LabelName): LabelId =
    LabelId(ctx.getOrCreateLabelId(label.name))

  implicit private def propertyToId(ctx: QueryContext)(property: PropertyKeyName): PropertyKeyId =
    PropertyKeyId(ctx.getOrCreatePropertyKeyId(property.name))

  implicit private def propertiesToIds(ctx: QueryContext)(properties: List[PropertyKeyName]): List[PropertyKeyId] =
    properties.map(property => PropertyKeyId(ctx.getOrCreatePropertyKeyId(property.name)))

  private def labelProp(ctx: QueryContext)(label: LabelName, prop: PropertyKeyName) =
    (ctx.getOrCreateLabelId(label.name), ctx.getOrCreatePropertyKeyId(prop.name))

  private def labelPropWithName(ctx: QueryContext)(label: LabelName, prop: PropertyKeyName, name: Option[String]) =
    (ctx.getOrCreateLabelId(label.name), ctx.getOrCreatePropertyKeyId(prop.name), name)

  private def typeProp(ctx: QueryContext)(relType: RelTypeName, prop: PropertyKeyName) =
    (ctx.getOrCreateRelTypeId(relType.name), ctx.getOrCreatePropertyKeyId(prop.name))

  private def typePropWithName(ctx: QueryContext)(relType: RelTypeName, prop: PropertyKeyName, name: Option[String]) =
    (ctx.getOrCreateRelTypeId(relType.name), ctx.getOrCreatePropertyKeyId(prop.name), name)
}
