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
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.logical.plans.ConstraintType
import org.neo4j.cypher.internal.logical.plans.CreateIndex
import org.neo4j.cypher.internal.logical.plans.CreateNodeKeyConstraint
import org.neo4j.cypher.internal.logical.plans.CreateNodePropertyExistenceConstraint
import org.neo4j.cypher.internal.logical.plans.CreateRelationshipPropertyExistenceConstraint
import org.neo4j.cypher.internal.logical.plans.CreateUniquePropertyConstraint
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExistsForConstraint
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExistsForIndex
import org.neo4j.cypher.internal.logical.plans.DropConstraintOnName
import org.neo4j.cypher.internal.logical.plans.DropIndex
import org.neo4j.cypher.internal.logical.plans.DropIndexOnName
import org.neo4j.cypher.internal.logical.plans.DropNodeKeyConstraint
import org.neo4j.cypher.internal.logical.plans.DropNodePropertyExistenceConstraint
import org.neo4j.cypher.internal.logical.plans.DropRelationshipPropertyExistenceConstraint
import org.neo4j.cypher.internal.logical.plans.DropUniquePropertyConstraint
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeKey
import org.neo4j.cypher.internal.logical.plans.NodePropertyExistence
import org.neo4j.cypher.internal.logical.plans.RelationshipPropertyExistence
import org.neo4j.cypher.internal.logical.plans.Uniqueness
import org.neo4j.cypher.internal.procs.IgnoredResult
import org.neo4j.cypher.internal.procs.SchemaWriteExecutionPlan
import org.neo4j.cypher.internal.procs.SuccessResult
import org.neo4j.cypher.internal.runtime.InternalQueryType
import org.neo4j.cypher.internal.runtime.ParameterMapping
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.SCHEMA_WRITE
import org.neo4j.cypher.internal.runtime.slottedParameters
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.internal.schema.ConstraintDescriptor

import scala.util.Try

/**
 * This runtime takes on queries that require no planning such as schema commands
 */
object SchemaCommandRuntime extends CypherRuntime[RuntimeContext] {
  override def name: String = "schema"

  override def compileToExecutable(state: LogicalQuery, context: RuntimeContext): ExecutionPlan = {

    val (withSlottedParameters, parameterMapping) = slottedParameters(state.logicalPlan)

    logicalToExecutable.applyOrElse(withSlottedParameters, throwCantCompile).apply(context, parameterMapping)
  }

  def throwCantCompile(unknownPlan: LogicalPlan): Nothing = {
    throw new CantCompileQueryException(
      s"Plan is not a schema command: ${unknownPlan.getClass.getSimpleName}")
  }

  def queryType(logicalPlan: LogicalPlan): Option[InternalQueryType] =
    if (logicalToExecutable.isDefinedAt(logicalPlan)) {
      Some(SCHEMA_WRITE)
    } else None

  val logicalToExecutable: PartialFunction[LogicalPlan, (RuntimeContext, ParameterMapping) => ExecutionPlan] = {
    // CREATE CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY
    case CreateNodeKeyConstraint(source, _, label, props, name) => (context, parameterMapping) =>
      SchemaWriteExecutionPlan("CreateNodeKeyConstraint", ctx => {
        val labelId = ctx.getOrCreateLabelId(label.name)
        val propertyKeyIds = props.map(p => propertyToId(ctx)(p.propertyKey).id)
        ctx.createNodeKeyConstraint(labelId, propertyKeyIds, name)
        SuccessResult
      }, source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping)))

    // DROP CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY
    case DropNodeKeyConstraint(label, props) => (_, _) =>
      SchemaWriteExecutionPlan("DropNodeKeyConstraint", ctx => {
        val labelId = ctx.getOrCreateLabelId(label.name)
        val propertyKeyIds = props.map(p => propertyToId(ctx)(p.propertyKey).id)
        ctx.dropNodeKeyConstraint(labelId, propertyKeyIds)
        SuccessResult
      })

    // CREATE CONSTRAINT ON (node:Label) ASSERT node.prop IS UNIQUE
    // CREATE CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS UNIQUE
    case CreateUniquePropertyConstraint(source, _, label, props, name) => (context, parameterMapping) =>
      SchemaWriteExecutionPlan("CreateUniqueConstraint", ctx => {
        val labelId = ctx.getOrCreateLabelId(label.name)
        val propertyKeyIds = props.map(p => propertyToId(ctx)(p.propertyKey).id)
        ctx.createUniqueConstraint(labelId, propertyKeyIds, name)
        SuccessResult
      }, source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping)))

    // DROP CONSTRAINT ON (node:Label) ASSERT node.prop IS UNIQUE
    // DROP CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS UNIQUE
    case DropUniquePropertyConstraint(label, props) => (_, _) =>
      SchemaWriteExecutionPlan("DropUniqueConstraint", ctx => {
        val labelId = ctx.getOrCreateLabelId(label.name)
        val propertyKeyIds = props.map(p => propertyToId(ctx)(p.propertyKey).id)
        ctx.dropUniqueConstraint(labelId, propertyKeyIds)
        SuccessResult
      })

    // CREATE CONSTRAINT ON (node:Label) ASSERT node.prop EXISTS
    case CreateNodePropertyExistenceConstraint(source, label, prop, name) => (context, parameterMapping) =>
      SchemaWriteExecutionPlan("CreateNodePropertyExistenceConstraint", ctx => {
        (ctx.createNodePropertyExistenceConstraint _).tupled(labelPropWithName(ctx)(label, prop.propertyKey, name))
        SuccessResult
      }, source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping)))

    // DROP CONSTRAINT ON (node:Label) ASSERT node.prop EXISTS
    case DropNodePropertyExistenceConstraint(label, prop) => (_, _) =>
      SchemaWriteExecutionPlan("DropNodePropertyExistenceConstraint", ctx => {
        (ctx.dropNodePropertyExistenceConstraint _).tupled(labelProp(ctx)(label, prop.propertyKey))
        SuccessResult
      })

    // CREATE CONSTRAINT ON ()-[r:R]-() ASSERT r.prop EXISTS
    case CreateRelationshipPropertyExistenceConstraint(source, relType, prop, name) => (context, parameterMapping) =>
      SchemaWriteExecutionPlan("CreateRelationshipPropertyExistenceConstraint", ctx => {
        (ctx.createRelationshipPropertyExistenceConstraint _).tupled(typePropWithName(ctx)(relType, prop.propertyKey, name))
        SuccessResult
      }, source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping)))

    // DROP CONSTRAINT ON ()-[r:R]-() ASSERT r.prop EXISTS
    case DropRelationshipPropertyExistenceConstraint(relType, prop) => (_, _) =>
      SchemaWriteExecutionPlan("DropRelationshipPropertyExistenceConstraint", ctx => {
        (ctx.dropRelationshipPropertyExistenceConstraint _).tupled(typeProp(ctx)(relType, prop.propertyKey))
        SuccessResult
      })

    case DropConstraintOnName(name, ifExists) => (_, _) =>
      SchemaWriteExecutionPlan("DropConstraint", ctx => {
        if (!ifExists || ctx.constraintExists(name)) {
          ctx.dropNamedConstraint(name)
        }
        SuccessResult
      })

    // CREATE INDEX ON :LABEL(prop)
    // CREATE INDEX FOR (n:LABEL) ON (n.prop)
    // CREATE INDEX name FOR (n:LABEL) ON (n.prop)
    case CreateIndex(source, label, props, name) => (context, parameterMapping) =>
      SchemaWriteExecutionPlan("CreateIndex", ctx => {
        val labelId = ctx.getOrCreateLabelId(label.name)
        val propertyKeyIds = props.map(p => propertyToId(ctx)(p).id)
        ctx.addIndexRule(labelId, propertyKeyIds, name)
        SuccessResult
      }, source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping)))

    // DROP INDEX ON :LABEL(prop)
    case DropIndex(label, props) => (_, _) =>
      SchemaWriteExecutionPlan("DropIndex", ctx => {
        val labelId = labelToId(ctx)(label)
        val propertyKeyIds = props.map(p => propertyToId(ctx)(p).id)
        ctx.dropIndexRule(labelId, propertyKeyIds)
        SuccessResult
      })

    // DROP INDEX name
    case DropIndexOnName(name, ifExists) => (_, _) =>
      SchemaWriteExecutionPlan("DropIndex", ctx => {
        if (!ifExists || ctx.indexExists(name)) {
          ctx.dropIndexRule(name)
        }
        SuccessResult
      })

    case DoNothingIfExistsForIndex(label, propertyKeyNames, name) => (_, _) =>
      SchemaWriteExecutionPlan("DoNothingIfNotExist", ctx => {
        val labelId = ctx.getOrCreateLabelId(label.name)
        val propertyKeyIds = propertyKeyNames.map(p => propertyToId(ctx)(p).id)
        if (Try(ctx.indexReference(labelId, propertyKeyIds: _*).getName).isSuccess) {
          IgnoredResult
        } else if (name.exists(ctx.indexExists)) {
          IgnoredResult
        } else {
          SuccessResult
        }
      }, None)

    case DoNothingIfExistsForConstraint(_, entityType, props, assertion, name) => (_, _) =>
      SchemaWriteExecutionPlan("DoNothingIfNotExist", ctx => {
        val entityId = entityType match {
          case Left(label) => ctx.getOrCreateLabelId(label.name)
          case Right(relType) => ctx.getOrCreateRelTypeId(relType.name)
        }
        val propertyKeyIds = props.map(p => propertyToId(ctx)(p.propertyKey).id)
        if (ctx.constraintExists(convertConstraintTypeToConstraintMatcher(assertion), entityId, propertyKeyIds: _*)) {
          IgnoredResult
        } else if (name.exists(ctx.constraintExists)) {
          IgnoredResult
        } else {
          SuccessResult
        }
      }, None)
  }

  def isApplicable(logicalPlanState: LogicalPlanState): Boolean = logicalToExecutable.isDefinedAt(logicalPlanState.maybeLogicalPlan.get)

  def convertConstraintTypeToConstraintMatcher(assertion: ConstraintType): ConstraintDescriptor => Boolean =
    assertion match {
      case NodePropertyExistence         => c => c.isNodePropertyExistenceConstraint
      case RelationshipPropertyExistence => c => c.isRelationshipPropertyExistenceConstraint
      case Uniqueness                    => c => c.isUniquenessConstraint
      case NodeKey                       => c => c.isNodeKeyConstraint
    }

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
