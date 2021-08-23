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
package org.neo4j.cypher.internal

import org.neo4j.common.EntityType
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.logical.plans.ConstraintType
import org.neo4j.cypher.internal.logical.plans.CreateBtreeIndex
import org.neo4j.cypher.internal.logical.plans.CreateFulltextIndex
import org.neo4j.cypher.internal.logical.plans.CreateLookupIndex
import org.neo4j.cypher.internal.logical.plans.CreateNodeKeyConstraint
import org.neo4j.cypher.internal.logical.plans.CreateNodePropertyExistenceConstraint
import org.neo4j.cypher.internal.logical.plans.CreateRelationshipPropertyExistenceConstraint
import org.neo4j.cypher.internal.logical.plans.CreateTextIndex
import org.neo4j.cypher.internal.logical.plans.CreateUniquePropertyConstraint
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExistsForBtreeIndex
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExistsForConstraint
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExistsForFulltextIndex
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExistsForLookupIndex
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExistsForTextIndex
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
import org.neo4j.cypher.internal.options.CypherRuntimeOption
import org.neo4j.cypher.internal.procs.IgnoredResult
import org.neo4j.cypher.internal.procs.SchemaExecutionPlan
import org.neo4j.cypher.internal.procs.SuccessResult
import org.neo4j.cypher.internal.runtime.InternalQueryType
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.SCHEMA_WRITE
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.internal.schema.ConstraintDescriptor
import org.neo4j.internal.schema.IndexConfig

import scala.language.implicitConversions
import scala.util.Try

/**
 * This runtime takes on queries that require no planning such as schema commands
 */
object SchemaCommandRuntime extends CypherRuntime[RuntimeContext] {
  override def name: String = "schema"

  override def correspondingRuntimeOption: Option[CypherRuntimeOption] = None

  override def compileToExecutable(state: LogicalQuery, context: RuntimeContext): ExecutionPlan = {
    logicalToExecutable.applyOrElse(state.logicalPlan, throwCantCompile).apply(context)
  }

  def throwCantCompile(unknownPlan: LogicalPlan): Nothing = {
    throw new CantCompileQueryException(
      s"Plan is not a schema command: ${unknownPlan.getClass.getSimpleName}")
  }

  def queryType(logicalPlan: LogicalPlan): Option[InternalQueryType] =
    if (logicalToExecutable.isDefinedAt(logicalPlan)) {
      Some(SCHEMA_WRITE)
    } else None

  val logicalToExecutable: PartialFunction[LogicalPlan, RuntimeContext => ExecutionPlan] = {
    // CREATE CONSTRAINT [name] [IF NOT EXISTS] ON (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY [OPTIONS {...}]
    case CreateNodeKeyConstraint(source, _, label, props, name, options) => context =>
      SchemaExecutionPlan("CreateNodeKeyConstraint", (ctx, params) => {
        val (indexProvider, indexConfig) = CreateBtreeIndexOptionsConverter("node key constraint").convert(options, params) match {
          case None => (None, IndexConfig.empty())
          case Some(CreateBtreeIndexOptions(provider, config)) => (provider, config)
        }
        val labelId = ctx.getOrCreateLabelId(label.name)
        val propertyKeyIds = props.map(p => propertyToId(ctx)(p.propertyKey).id)
        ctx.createNodeKeyConstraint(labelId, propertyKeyIds, name, indexProvider, indexConfig)
        SuccessResult
      }, source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context)))

    // DROP CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY
    case DropNodeKeyConstraint(label, props) => _ =>
      SchemaExecutionPlan("DropNodeKeyConstraint", (ctx, _) => {
        val labelId = ctx.getOrCreateLabelId(label.name)
        val propertyKeyIds = props.map(p => propertyToId(ctx)(p.propertyKey).id)
        ctx.dropNodeKeyConstraint(labelId, propertyKeyIds)
        SuccessResult
      })

    // CREATE CONSTRAINT [name] [IF NOT EXISTS] ON (node:Label) ASSERT node.prop IS UNIQUE [OPTIONS {...}]
    // CREATE CONSTRAINT [name] [IF NOT EXISTS] ON (node:Label) ASSERT (node.prop1,node.prop2) IS UNIQUE [OPTIONS {...}]
    case CreateUniquePropertyConstraint(source, _, label, props, name, options) => context =>
      SchemaExecutionPlan("CreateUniqueConstraint", (ctx, params) => {
        val (indexProvider, indexConfig) = CreateBtreeIndexOptionsConverter("uniqueness constraint").convert(options, params) match {
          case None => (None, IndexConfig.empty())
          case Some(CreateBtreeIndexOptions(provider, config)) => (provider, config)
        }
        val labelId = ctx.getOrCreateLabelId(label.name)
        val propertyKeyIds = props.map(p => propertyToId(ctx)(p.propertyKey).id)
        ctx.createUniqueConstraint(labelId, propertyKeyIds, name, indexProvider, indexConfig)
        SuccessResult
      }, source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context)))

    // DROP CONSTRAINT ON (node:Label) ASSERT node.prop IS UNIQUE
    // DROP CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS UNIQUE
    case DropUniquePropertyConstraint(label, props) => _ =>
      SchemaExecutionPlan("DropUniqueConstraint", (ctx, _) => {
        val labelId = ctx.getOrCreateLabelId(label.name)
        val propertyKeyIds = props.map(p => propertyToId(ctx)(p.propertyKey).id)
        ctx.dropUniqueConstraint(labelId, propertyKeyIds)
        SuccessResult
      })

    // CREATE CONSTRAINT [name] [IF NOT EXISTS] ON (node:Label) ASSERT node.prop IS NOT NULL
    case CreateNodePropertyExistenceConstraint(source, label, prop, name, options) => context =>
      SchemaExecutionPlan("CreateNodePropertyExistenceConstraint", (ctx, params) => {
        PropertyExistenceConstraintOptionsConverter("node").convert(options, params) // Assert empty options
        (ctx.createNodePropertyExistenceConstraint _).tupled(labelPropWithName(ctx)(label, prop.propertyKey, name))
        SuccessResult
      }, source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context)))

    // DROP CONSTRAINT ON (node:Label) ASSERT EXISTS (node.prop)
    case DropNodePropertyExistenceConstraint(label, prop) => _ =>
      SchemaExecutionPlan("DropNodePropertyExistenceConstraint", (ctx, _) => {
        (ctx.dropNodePropertyExistenceConstraint _).tupled(labelProp(ctx)(label, prop.propertyKey))
        SuccessResult
      })

    // CREATE CONSTRAINT [name] [IF NOT EXISTS] ON ()-[r:R]-() ASSERT r.prop IS NOT NULL
    case CreateRelationshipPropertyExistenceConstraint(source, relType, prop, name, options) => context =>
      SchemaExecutionPlan("CreateRelationshipPropertyExistenceConstraint", (ctx, params) => {
        PropertyExistenceConstraintOptionsConverter("relationship").convert(options, params) // Assert empty options
        (ctx.createRelationshipPropertyExistenceConstraint _).tupled(typePropWithName(ctx)(relType, prop.propertyKey, name))
        SuccessResult
      }, source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context)))

    // DROP CONSTRAINT ON ()-[r:R]-() ASSERT EXISTS (r.prop)
    case DropRelationshipPropertyExistenceConstraint(relType, prop) => _ =>
      SchemaExecutionPlan("DropRelationshipPropertyExistenceConstraint", (ctx, _) => {
        (ctx.dropRelationshipPropertyExistenceConstraint _).tupled(typeProp(ctx)(relType, prop.propertyKey))
        SuccessResult
      })

    // DROP CONSTRAINT name [IF EXISTS]
    case DropConstraintOnName(name, ifExists) => _ =>
      SchemaExecutionPlan("DropConstraint", (ctx, _) => {
        if (!ifExists || ctx.constraintExists(name)) {
          ctx.dropNamedConstraint(name)
        }
        SuccessResult
      })

    // CREATE INDEX ON :LABEL(prop)
    // CREATE INDEX [name] [IF NOT EXISTS] FOR (n:LABEL) ON (n.prop) [OPTIONS {...}]
    // CREATE INDEX [name] [IF NOT EXISTS] FOR ()-[n:TYPE]-() ON (n.prop) [OPTIONS {...}]
    case CreateBtreeIndex(source, entityName, props, name, options) => context =>
      SchemaExecutionPlan("CreateIndex", (ctx, params) => {
        val (entityId, entityType) = getEntityInfo(entityName, ctx)
        val schemaType = entityType match {
          case EntityType.NODE => "btree node index"
          case EntityType.RELATIONSHIP =>"btree relationship index"
        }
        val (indexProvider, indexConfig) = CreateBtreeIndexOptionsConverter(schemaType).convert(options, params) match {
          case None => (None, IndexConfig.empty())
          case Some(CreateBtreeIndexOptions(provider, config)) => (provider, config)
        }
        val propertyKeyIds = props.map(p => propertyToId(ctx)(p).id)
        ctx.addBtreeIndexRule(entityId, entityType, propertyKeyIds, name, indexProvider, indexConfig)
        SuccessResult
      }, source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context)))

    // CREATE LOOKUP INDEX [name] [IF NOT EXISTS] FOR (n) ON EACH labels(n)
    // CREATE LOOKUP INDEX [name] [IF NOT EXISTS] FOR ()-[r]-() ON [EACH] type(r)
    case CreateLookupIndex(source, entityType, name, options) => context =>
      SchemaExecutionPlan("CreateIndex", (ctx, params) => {
        val provider = CreateLookupIndexOptionsConverter.convert(options, params).flatMap(_.provider)
        ctx.addLookupIndexRule(entityType, name, provider)
        SuccessResult
      }, source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context)))

    // CREATE FULLTEXT INDEX [name] [IF NOT EXISTS] FOR (n:LABEL) ON EACH (n.prop) [OPTIONS {...}]
    // CREATE FULLTEXT INDEX [name] [IF NOT EXISTS] FOR ()-[n:TYPE]-() ON EACH (n.prop) [OPTIONS {...}]
    case CreateFulltextIndex(source, entityNames, props, name, options) => context =>
      SchemaExecutionPlan("CreateIndex", (ctx, params) => {
        val (indexProvider, indexConfig) = CreateFulltextIndexOptionsConverter.convert(options, params) match {
          case None => (None, IndexConfig.empty())
          case Some(CreateFulltextIndexOptions(provider, config)) => (provider, config)
        }
        val (entityIds, entityType) = getMultipleEntityInfo(entityNames, ctx)
        val propertyKeyIds = props.map(p => propertyToId(ctx)(p).id)
        ctx.addFulltextIndexRule(entityIds, entityType, propertyKeyIds, name, indexProvider, indexConfig)
        SuccessResult
      }, source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context)))

    // CREATE TEXT INDEX [name] [IF NOT EXISTS] FOR (n:LABEL) ON (n.prop) [OPTIONS {...}]
    // CREATE TEXT INDEX [name] [IF NOT EXISTS] FOR ()-[n:TYPE]-() ON (n.prop) [OPTIONS {...}]
    case CreateTextIndex(source, entityName, props, name, options) => context =>
      SchemaExecutionPlan("CreateIndex", (ctx, params) => {
        val (entityId, entityType) = getEntityInfo(entityName, ctx)
        val propertyKeyIds = props.map(p => propertyToId(ctx)(p).id)
        val provider = CreateTextIndexOptionsConverter.convert(options, params).flatMap(_.provider)
        ctx.addTextIndexRule(entityId, entityType, propertyKeyIds, name, provider)
        SuccessResult
      }, source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context)))

    // DROP INDEX ON :LABEL(prop)
    case DropIndex(label, props) => _ =>
      SchemaExecutionPlan("DropIndex", (ctx, _)=> {
        val labelId = labelToId(ctx)(label)
        val propertyKeyIds = props.map(p => propertyToId(ctx)(p).id)
        ctx.dropIndexRule(labelId, propertyKeyIds)
        SuccessResult
      })

    // DROP INDEX name [IF EXISTS]
    case DropIndexOnName(name, ifExists) => _ =>
      SchemaExecutionPlan("DropIndex", (ctx, _) => {
        if (!ifExists || ctx.indexExists(name)) {
          ctx.dropIndexRule(name)
        }
        SuccessResult
      })

    case DoNothingIfExistsForBtreeIndex(entityName, propertyKeyNames, name) => _ =>
      SchemaExecutionPlan("DoNothingIfExist", (ctx, _) => {
        val (entityId, entityType) = getEntityInfo(entityName, ctx)
        val propertyKeyIds = propertyKeyNames.map(p => propertyToId(ctx)(p).id)
        if (Try(ctx.btreeIndexReference(entityId, entityType, propertyKeyIds: _*).getName).isSuccess) {
          IgnoredResult
        } else if (name.exists(ctx.indexExists)) {
          IgnoredResult
        } else {
          SuccessResult
        }
      }, None)

    case DoNothingIfExistsForLookupIndex(entityType, name) => _ =>
      SchemaExecutionPlan("DoNothingIfExist", (ctx, _) => {
        if (Try(ctx.lookupIndexReference(entityType).getName).isSuccess) {
          IgnoredResult
        } else if (name.exists(ctx.indexExists)) {
          IgnoredResult
        } else {
          SuccessResult
        }
      }, None)

    case DoNothingIfExistsForFulltextIndex(entityNames, propertyKeyNames, name) => _ =>
      SchemaExecutionPlan("DoNothingIfExist", (ctx, _) => {
        val (entityIds, entityType) = getMultipleEntityInfo(entityNames, ctx)
        val propertyKeyIds = propertyKeyNames.map(p => propertyToId(ctx)(p).id)
        if (Try(ctx.fulltextIndexReference(entityIds, entityType, propertyKeyIds: _*).getName).isSuccess) {
          IgnoredResult
        } else if (name.exists(ctx.indexExists)) {
          IgnoredResult
        } else {
          SuccessResult
        }
      }, None)

    case DoNothingIfExistsForTextIndex(entityName, propertyKeyNames, name) => _ =>
      SchemaExecutionPlan("DoNothingIfExist", (ctx, _) => {
        val (entityId, entityType) = getEntityInfo(entityName, ctx)
        val propertyKeyIds = propertyKeyNames.map(p => propertyToId(ctx)(p).id)
        if (Try(ctx.textIndexReference(entityId, entityType, propertyKeyIds: _*).getName).isSuccess) {
          IgnoredResult
        } else if (name.exists(ctx.indexExists)) {
          IgnoredResult
        } else {
          SuccessResult
        }
      }, None)

    case DoNothingIfExistsForConstraint(_, entityName, props, assertion, name) => _ =>
      SchemaExecutionPlan("DoNothingIfExist", (ctx, _) => {
        val (entityId, _) = getEntityInfo(entityName, ctx)
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

  private def getEntityInfo(entityName: Either[LabelName, RelTypeName], ctx: QueryContext) = entityName match {
    // returns (entityId, EntityType)
    case Left(label)    => (ctx.getOrCreateLabelId(label.name), EntityType.NODE)
    case Right(relType) => (ctx.getOrCreateRelTypeId(relType.name), EntityType.RELATIONSHIP)
  }

  private def getMultipleEntityInfo(entityName: Either[List[LabelName], List[RelTypeName]], ctx: QueryContext) = entityName match {
    // returns (entityIds, EntityType)
    case Left(labels)    => (labels.map(label => ctx.getOrCreateLabelId(label.name)), EntityType.NODE)
    case Right(relTypes) => (relTypes.map(relType => ctx.getOrCreateRelTypeId(relType.name)), EntityType.RELATIONSHIP)
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

  private def labelProp(ctx: QueryContext)(label: LabelName, prop: PropertyKeyName) =
    (ctx.getOrCreateLabelId(label.name), ctx.getOrCreatePropertyKeyId(prop.name))

  private def labelPropWithName(ctx: QueryContext)(label: LabelName, prop: PropertyKeyName, name: Option[String]) =
    (ctx.getOrCreateLabelId(label.name), ctx.getOrCreatePropertyKeyId(prop.name), name)

  private def typeProp(ctx: QueryContext)(relType: RelTypeName, prop: PropertyKeyName) =
    (ctx.getOrCreateRelTypeId(relType.name), ctx.getOrCreatePropertyKeyId(prop.name))

  private def typePropWithName(ctx: QueryContext)(relType: RelTypeName, prop: PropertyKeyName, name: Option[String]) =
    (ctx.getOrCreateRelTypeId(relType.name), ctx.getOrCreatePropertyKeyId(prop.name), name)
}
