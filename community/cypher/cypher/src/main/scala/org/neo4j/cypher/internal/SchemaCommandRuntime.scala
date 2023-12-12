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
package org.neo4j.cypher.internal

import org.neo4j.common.EntityType
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.expressions.ElementTypeName
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.logical.plans.ConstraintType
import org.neo4j.cypher.internal.logical.plans.CreateConstraint
import org.neo4j.cypher.internal.logical.plans.CreateFulltextIndex
import org.neo4j.cypher.internal.logical.plans.CreateIndex
import org.neo4j.cypher.internal.logical.plans.CreateLookupIndex
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExistsForConstraint
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExistsForFulltextIndex
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExistsForIndex
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExistsForLookupIndex
import org.neo4j.cypher.internal.logical.plans.DropConstraintOnName
import org.neo4j.cypher.internal.logical.plans.DropIndexOnName
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeKey
import org.neo4j.cypher.internal.logical.plans.NodePropertyExistence
import org.neo4j.cypher.internal.logical.plans.NodePropertyType
import org.neo4j.cypher.internal.logical.plans.NodeUniqueness
import org.neo4j.cypher.internal.logical.plans.RelationshipKey
import org.neo4j.cypher.internal.logical.plans.RelationshipPropertyExistence
import org.neo4j.cypher.internal.logical.plans.RelationshipPropertyType
import org.neo4j.cypher.internal.logical.plans.RelationshipUniqueness
import org.neo4j.cypher.internal.options.CypherRuntimeOption
import org.neo4j.cypher.internal.procs.IgnoredResult
import org.neo4j.cypher.internal.procs.PropertyTypeMapper
import org.neo4j.cypher.internal.procs.SchemaExecutionPlan
import org.neo4j.cypher.internal.procs.SuccessResult
import org.neo4j.cypher.internal.runtime.InternalQueryType
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.SCHEMA_WRITE
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.exceptions.ParameterWrongTypeException
import org.neo4j.graphdb.schema.IndexType.POINT
import org.neo4j.graphdb.schema.IndexType.RANGE
import org.neo4j.graphdb.schema.IndexType.TEXT
import org.neo4j.graphdb.schema.IndexType.VECTOR
import org.neo4j.internal.schema.ConstraintDescriptor
import org.neo4j.internal.schema.IndexConfig
import org.neo4j.internal.schema.IndexType
import org.neo4j.internal.schema.constraints.PropertyTypeSet
import org.neo4j.values.storable.StringValue
import org.neo4j.values.virtual.MapValue

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
      s"Plan is not a schema command: ${unknownPlan.getClass.getSimpleName}"
    )
  }

  def queryType(logicalPlan: LogicalPlan): Option[InternalQueryType] =
    if (logicalToExecutable.isDefinedAt(logicalPlan)) {
      Some(SCHEMA_WRITE)
    } else None

  val logicalToExecutable: PartialFunction[LogicalPlan, RuntimeContext => ExecutionPlan] = {
    // CREATE CONSTRAINT [name] [IF NOT EXISTS] FOR (node:Label) REQUIRE (node.prop1,node.prop2) IS NODE KEY [OPTIONS {...}]
    case CreateConstraint(source, NodeKey, label: LabelName, props, name, options) => context =>
        SchemaExecutionPlan(
          "CreateNodeKeyConstraint",
          (ctx, params) => {
            val constraintName = getName(name, params)
            val indexProvider =
              IndexBackedConstraintsOptionsConverter("node key constraint", ctx)
                .convert(options, params)
                .flatMap(_.provider)
            val labelId = ctx.getOrCreateLabelId(label.name)
            val propertyKeyIds = props.map(p => propertyToId(ctx)(p.propertyKey).id)
            ctx.createNodeKeyConstraint(labelId, propertyKeyIds, constraintName, indexProvider)
            SuccessResult
          },
          source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context))
        )

    // CREATE CONSTRAINT [name] [IF NOT EXISTS] FOR ()-[rel:TYPE]-() REQUIRE (rel.prop1,rel.prop2) IS RELATIONSHIP KEY [OPTIONS {...}]
    case CreateConstraint(source, RelationshipKey, relType: RelTypeName, props, name, options) => context =>
        SchemaExecutionPlan(
          "CreateRelationshipKeyConstraint",
          (ctx, params) => {
            val constraintName = getName(name, params)
            val indexProvider =
              IndexBackedConstraintsOptionsConverter("relationship key constraint", ctx)
                .convert(options, params)
                .flatMap(_.provider)
            val relId = ctx.getOrCreateRelTypeId(relType.name)
            val propertyKeyIds = props.map(p => propertyToId(ctx)(p.propertyKey).id)
            ctx.createRelationshipKeyConstraint(relId, propertyKeyIds, constraintName, indexProvider)
            SuccessResult
          },
          source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context))
        )

      // CREATE CONSTRAINT [name] [IF NOT EXISTS] FOR (node:Label) REQUIRE node.prop IS UNIQUE [OPTIONS {...}]
    // CREATE CONSTRAINT [name] [IF NOT EXISTS] FOR (node:Label) REQUIRE (node.prop1,node.prop2) IS UNIQUE [OPTIONS {...}]
    case CreateConstraint(source, NodeUniqueness, label: LabelName, props, name, options) => context =>
        SchemaExecutionPlan(
          "CreateNodePropertyUniquenessConstraint",
          (ctx, params) => {
            val constraintName = getName(name, params)
            val indexProvider =
              IndexBackedConstraintsOptionsConverter("uniqueness constraint", ctx)
                .convert(options, params)
                .flatMap(_.provider)
            val labelId = ctx.getOrCreateLabelId(label.name)
            val propertyKeyIds = props.map(p => propertyToId(ctx)(p.propertyKey).id)
            ctx.createNodeUniqueConstraint(labelId, propertyKeyIds, constraintName, indexProvider)
            SuccessResult
          },
          source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context))
        )

      // CREATE CONSTRAINT [name] [IF NOT EXISTS] FOR ()-[rel:TYPE]-() REQUIRE rel.prop IS UNIQUE [OPTIONS {...}]
    // CREATE CONSTRAINT [name] [IF NOT EXISTS] FOR ()-[rel:TYPE]-() REQUIRE (rel.prop1,rel.prop2) IS UNIQUE [OPTIONS {...}]
    case CreateConstraint(source, RelationshipUniqueness, relType: RelTypeName, props, name, options) => context =>
        SchemaExecutionPlan(
          "CreateRelationshipPropertyUniquenessConstraint",
          (ctx, params) => {
            val constraintName = getName(name, params)
            val indexProvider =
              IndexBackedConstraintsOptionsConverter("relationship uniqueness constraint", ctx)
                .convert(options, params)
                .flatMap(_.provider)
            val relTypeId = ctx.getOrCreateRelTypeId(relType.name)
            val propertyKeyIds = props.map(p => propertyToId(ctx)(p.propertyKey).id)
            ctx.createRelationshipUniqueConstraint(relTypeId, propertyKeyIds, constraintName, indexProvider)
            SuccessResult
          },
          source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context))
        )

    // CREATE CONSTRAINT [name] [IF NOT EXISTS] FOR (node:Label) REQUIRE node.prop IS NOT NULL
    case CreateConstraint(source, NodePropertyExistence, label: LabelName, prop, name, options) => context =>
        SchemaExecutionPlan(
          "CreateNodePropertyExistenceConstraint",
          (ctx, params) => {
            val constraintName = getName(name, params)
            // Assert empty options
            PropertyExistenceOrTypeConstraintOptionsConverter("node", "existence", ctx).convert(options, params)
            (ctx.createNodePropertyExistenceConstraint _).tupled(labelPropWithName(ctx)(
              label,
              prop.head.propertyKey,
              constraintName
            ))
            SuccessResult
          },
          source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context))
        )

    // CREATE CONSTRAINT [name] [IF NOT EXISTS] FOR ()-[r:R]-() REQUIRE r.prop IS NOT NULL
    case CreateConstraint(source, RelationshipPropertyExistence, relType: RelTypeName, prop, name, options) =>
      context =>
        SchemaExecutionPlan(
          "CreateRelationshipPropertyExistenceConstraint",
          (ctx, params) => {
            val constraintName = getName(name, params)
            // Assert empty options
            PropertyExistenceOrTypeConstraintOptionsConverter("relationship", "existence", ctx).convert(options, params)
            (ctx.createRelationshipPropertyExistenceConstraint _).tupled(typePropWithName(ctx)(
              relType,
              prop.head.propertyKey,
              constraintName
            ))
            SuccessResult
          },
          source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context))
        )

    // CREATE CONSTRAINT [name] [IF NOT EXISTS] FOR (node:Label) REQUIRE node.prop IS TYPED ...
    case CreateConstraint(source, NodePropertyType(propertyType), label: LabelName, prop, name, options) => context =>
        SchemaExecutionPlan(
          "CreateNodePropertyTypeConstraint",
          (ctx, params) => {
            val constraintName = getName(name, params)
            // Assert empty options
            PropertyExistenceOrTypeConstraintOptionsConverter("node", "type", ctx).convert(options, params)
            val (labelId, propId, _) = labelPropWithName(ctx)(label, prop.head.propertyKey, constraintName)
            ctx.createNodePropertyTypeConstraint(
              labelId,
              propId,
              PropertyTypeMapper.asPropertyTypeSet(propertyType),
              constraintName
            )
            SuccessResult
          },
          source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context))
        )

    // CREATE CONSTRAINT [name] [IF NOT EXISTS] FOR ()-[r:R]-() REQUIRE r.prop IS TYPED ...
    case CreateConstraint(source, RelationshipPropertyType(propertyType), relType: RelTypeName, prop, name, options) =>
      context =>
        SchemaExecutionPlan(
          "CreateRelationshipPropertyTypeConstraint",
          (ctx, params) => {
            val constraintName = getName(name, params)
            // Assert empty options
            PropertyExistenceOrTypeConstraintOptionsConverter("relationship", "type", ctx).convert(options, params)
            val (relTypeId, propId, _) = typePropWithName(ctx)(relType, prop.head.propertyKey, constraintName)
            ctx.createRelationshipPropertyTypeConstraint(
              relTypeId,
              propId,
              PropertyTypeMapper.asPropertyTypeSet(propertyType),
              constraintName
            )
            SuccessResult
          },
          source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context))
        )

    // DROP CONSTRAINT name [IF EXISTS]
    case DropConstraintOnName(name, ifExists) => _ =>
        SchemaExecutionPlan(
          "DropConstraint",
          (ctx, params) => {
            val constraintName = getName(name, params)
            if (!ifExists || ctx.constraintExists(constraintName)) {
              ctx.dropNamedConstraint(constraintName)
            }
            SuccessResult
          }
        )

      // CREATE [RANGE] INDEX [name] [IF NOT EXISTS] FOR (n:LABEL) ON (n.prop) [OPTIONS {...}]
    // CREATE [RANGE] INDEX [name] [IF NOT EXISTS] FOR ()-[n:TYPE]-() ON (n.prop) [OPTIONS {...}]
    case CreateIndex(source, RANGE, entityName, props, name, options) => context =>
        SchemaExecutionPlan(
          "CreateIndex",
          (ctx, params) => {
            val indexName = getName(name, params)
            val (entityId, entityType) = getEntityInfo(entityName, ctx)
            val schemaType = entityType match {
              case EntityType.NODE         => "range node property index"
              case EntityType.RELATIONSHIP => "range relationship property index"
            }
            val provider =
              CreateRangeIndexOptionsConverter(schemaType, ctx).convert(options, params).flatMap(_.provider)
            val propertyKeyIds = props.map(p => propertyToId(ctx)(p).id)
            ctx.addRangeIndexRule(entityId, entityType, propertyKeyIds, indexName, provider)
            SuccessResult
          },
          source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context))
        )

      // CREATE LOOKUP INDEX [name] [IF NOT EXISTS] FOR (n) ON EACH labels(n)
    // CREATE LOOKUP INDEX [name] [IF NOT EXISTS] FOR ()-[r]-() ON [EACH] type(r)
    case CreateLookupIndex(source, entityType, name, options) => context =>
        SchemaExecutionPlan(
          "CreateIndex",
          (ctx, params) => {
            val indexName = getName(name, params)
            val provider = CreateLookupIndexOptionsConverter(ctx).convert(options, params).flatMap(_.provider)
            ctx.addLookupIndexRule(entityType, indexName, provider)
            SuccessResult
          },
          source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context))
        )

      // CREATE FULLTEXT INDEX [name] [IF NOT EXISTS] FOR (n:LABEL) ON EACH (n.prop) [OPTIONS {...}]
    // CREATE FULLTEXT INDEX [name] [IF NOT EXISTS] FOR ()-[n:TYPE]-() ON EACH (n.prop) [OPTIONS {...}]
    case CreateFulltextIndex(source, entityNames, props, name, options) => context =>
        SchemaExecutionPlan(
          "CreateIndex",
          (ctx, params) => {
            val indexName = getName(name, params)
            val (indexProvider, indexConfig) = CreateFulltextIndexOptionsConverter(ctx).convert(options, params) match {
              case None                                               => (None, IndexConfig.empty())
              case Some(CreateIndexWithFullOptions(provider, config)) => (provider, config)
            }
            val (entityIds, entityType) = getMultipleEntityInfo(entityNames, ctx)
            val propertyKeyIds = props.map(p => propertyToId(ctx)(p).id)
            ctx.addFulltextIndexRule(entityIds, entityType, propertyKeyIds, indexName, indexProvider, indexConfig)
            SuccessResult
          },
          source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context))
        )

      // CREATE TEXT INDEX [name] [IF NOT EXISTS] FOR (n:LABEL) ON (n.prop) [OPTIONS {...}]
    // CREATE TEXT INDEX [name] [IF NOT EXISTS] FOR ()-[n:TYPE]-() ON (n.prop) [OPTIONS {...}]
    case CreateIndex(source, TEXT, entityName, props, name, options) => context =>
        SchemaExecutionPlan(
          "CreateIndex",
          (ctx, params) => {
            val indexName = getName(name, params)
            val provider = CreateTextIndexOptionsConverter(ctx).convert(options, params).flatMap(_.provider)
            val (entityId, entityType) = getEntityInfo(entityName, ctx)
            val propertyKeyIds = props.map(p => propertyToId(ctx)(p).id)
            ctx.addTextIndexRule(entityId, entityType, propertyKeyIds, indexName, provider)
            SuccessResult
          },
          source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context))
        )

      // CREATE POINT INDEX [name] [IF NOT EXISTS] FOR (n:LABEL) ON (n.prop) [OPTIONS {...}]
    // CREATE POINT INDEX [name] [IF NOT EXISTS] FOR ()-[n:TYPE]-() ON (n.prop) [OPTIONS {...}]
    case CreateIndex(source, POINT, entityName, props, name, options) => context =>
        SchemaExecutionPlan(
          "CreateIndex",
          (ctx, params) => {
            val indexName = getName(name, params)
            val (indexProvider, indexConfig) = CreatePointIndexOptionsConverter(ctx).convert(options, params) match {
              case None                                               => (None, IndexConfig.empty())
              case Some(CreateIndexWithFullOptions(provider, config)) => (provider, config)
            }
            val (entityId, entityType) = getEntityInfo(entityName, ctx)
            val propertyKeyIds = props.map(p => propertyToId(ctx)(p).id)
            ctx.addPointIndexRule(entityId, entityType, propertyKeyIds, indexName, indexProvider, indexConfig)
            SuccessResult
          },
          source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context))
        )

    // CREATE VECTOR INDEX [name] [IF NOT EXISTS] FOR (n:LABEL) ON (n.prop) OPTIONS {...}
    case CreateIndex(source, VECTOR, entityName, props, name, options) => context =>
        SchemaExecutionPlan(
          "CreateIndex",
          (ctx, params) => {
            val indexName = getName(name, params)
            val optionsConverter = CreateVectorIndexOptionsConverter(ctx)
            val (indexProvider, indexConfig) = optionsConverter.convert(options, params) match {
              case None =>
                // will not get here due to mandatory config but need this to make it compile
                (None, IndexConfig.empty())
              case Some(CreateIndexWithFullOptions(provider, config)) => (provider, config)
            }
            val (entityId, entityType) = getEntityInfo(entityName, ctx)
            val propertyKeyIds = props.map(p => propertyToId(ctx)(p).id)
            ctx.addVectorIndexRule(entityId, entityType, propertyKeyIds, indexName, indexProvider, indexConfig)
            SuccessResult
          },
          source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context))
        )

    // DROP INDEX name [IF EXISTS]
    case DropIndexOnName(name, ifExists) => _ =>
        SchemaExecutionPlan(
          "DropIndex",
          (ctx, params) => {
            val indexName = getName(name, params)
            if (!ifExists || ctx.indexExists(indexName)) {
              ctx.dropIndexRule(indexName)
            }
            SuccessResult
          }
        )

    case DoNothingIfExistsForIndex(entityName, propertyKeyNames, indexType, name, options) => _ =>
        val (innerIndexType, optionsConverter) = indexType match {
          case POINT  => (IndexType.POINT, CreatePointIndexOptionsConverter)
          case RANGE  => (IndexType.RANGE, ctx => CreateRangeIndexOptionsConverter("range index", ctx))
          case TEXT   => (IndexType.TEXT, CreateTextIndexOptionsConverter)
          case VECTOR => (IndexType.VECTOR, CreateVectorIndexOptionsConverter)
          case it =>
            throw new IllegalStateException(
              s"Did not expect index type $it here: only point, range, text or vector indexes."
            )
        }
        SchemaExecutionPlan(
          "DoNothingIfExist",
          (ctx, params) => {
            val indexName = getName(name, params)
            // Assert correct options to get errors even if matching index already exists
            optionsConverter(ctx).convert(options, params)

            val (entityId, entityType) = getEntityInfo(entityName, ctx)
            val propertyKeyIds = propertyKeyNames.map(p => propertyToId(ctx)(p).id)
            if (Try(ctx.indexReference(innerIndexType, entityId, entityType, propertyKeyIds: _*).getName).isSuccess) {
              IgnoredResult
            } else if (indexName.exists(ctx.indexExists)) {
              IgnoredResult
            } else {
              SuccessResult
            }
          },
          None
        )

    case DoNothingIfExistsForLookupIndex(entityType, name, options) => _ =>
        SchemaExecutionPlan(
          "DoNothingIfExist",
          (ctx, params) => {
            val indexName = getName(name, params)
            // Assert correct options to get errors even if matching index already exists
            CreateLookupIndexOptionsConverter(ctx).convert(options, params)

            if (Try(ctx.lookupIndexReference(entityType).getName).isSuccess) {
              IgnoredResult
            } else if (indexName.exists(ctx.indexExists)) {
              IgnoredResult
            } else {
              SuccessResult
            }
          },
          None
        )

    case DoNothingIfExistsForFulltextIndex(entityNames, propertyKeyNames, name, options) => _ =>
        SchemaExecutionPlan(
          "DoNothingIfExist",
          (ctx, params) => {
            val indexName = getName(name, params)
            // Assert correct options to get errors even if matching index already exists
            CreateFulltextIndexOptionsConverter(ctx).convert(options, params)

            val (entityIds, entityType) = getMultipleEntityInfo(entityNames, ctx)
            val propertyKeyIds = propertyKeyNames.map(p => propertyToId(ctx)(p).id)
            if (Try(ctx.fulltextIndexReference(entityIds, entityType, propertyKeyIds: _*).getName).isSuccess) {
              IgnoredResult
            } else if (indexName.exists(ctx.indexExists)) {
              IgnoredResult
            } else {
              SuccessResult
            }
          },
          None
        )

    case DoNothingIfExistsForConstraint(entityName, props, assertion, name, options) => _ =>
        SchemaExecutionPlan(
          "DoNothingIfExist",
          (ctx, params) => {
            val constraintName = getName(name, params)
            // Assert correct options to get errors even if matching constraint already exists
            assertion match {
              case NodeKey =>
                IndexBackedConstraintsOptionsConverter("node key constraint", ctx).convert(options, params)
              case RelationshipKey =>
                IndexBackedConstraintsOptionsConverter("relationship key constraint", ctx).convert(options, params)
              case NodeUniqueness =>
                IndexBackedConstraintsOptionsConverter("uniqueness constraint", ctx).convert(options, params)
              case RelationshipUniqueness =>
                IndexBackedConstraintsOptionsConverter("relationship uniqueness constraint", ctx)
                  .convert(options, params)
              case NodePropertyExistence =>
                PropertyExistenceOrTypeConstraintOptionsConverter("node", "existence", ctx).convert(options, params)
              case RelationshipPropertyExistence =>
                PropertyExistenceOrTypeConstraintOptionsConverter("relationship", "existence", ctx)
                  .convert(options, params)
              case _: NodePropertyType =>
                PropertyExistenceOrTypeConstraintOptionsConverter("node", "type", ctx).convert(options, params)
              case _: RelationshipPropertyType =>
                PropertyExistenceOrTypeConstraintOptionsConverter("relationship", "type", ctx).convert(options, params)
            }

            val (entityId, _) = getEntityInfo(entityName, ctx)
            val propertyKeyIds = props.map(p => propertyToId(ctx)(p.propertyKey).id)
            if (
              ctx.constraintExists(convertConstraintTypeToConstraintMatcher(assertion), entityId, propertyKeyIds: _*)
            ) {
              IgnoredResult
            } else if (constraintName.exists(ctx.constraintExists)) {
              IgnoredResult
            } else {
              SuccessResult
            }
          },
          None
        )
  }

  private def getName(name: Option[Either[String, Parameter]], params: MapValue): Option[String] =
    name.map(getName(_, params))

  private def getName(name: Either[String, Parameter], params: MapValue): String = name match {
    case Left(stringName) => stringName
    case Right(paramName) =>
      params.get(paramName.name) match {
        case s: StringValue => s.stringValue()
        case x =>
          throw new ParameterWrongTypeException(s"Expected String, but got: ${x.getTypeName}")
      }
  }

  private def getEntityInfo(entityName: ElementTypeName, ctx: QueryContext) = entityName match {
    // returns (entityId, EntityType)
    case label: LabelName     => (ctx.getOrCreateLabelId(label.name), EntityType.NODE)
    case relType: RelTypeName => (ctx.getOrCreateRelTypeId(relType.name), EntityType.RELATIONSHIP)
  }

  private def getMultipleEntityInfo(entityName: Either[List[LabelName], List[RelTypeName]], ctx: QueryContext) =
    entityName match {
      // returns (entityIds, EntityType)
      case Left(labels)    => (labels.map(label => ctx.getOrCreateLabelId(label.name)), EntityType.NODE)
      case Right(relTypes) => (relTypes.map(relType => ctx.getOrCreateRelTypeId(relType.name)), EntityType.RELATIONSHIP)
    }

  def isApplicable(logicalPlanState: LogicalPlanState): Boolean =
    logicalToExecutable.isDefinedAt(logicalPlanState.maybeLogicalPlan.get)

  def convertConstraintTypeToConstraintMatcher(assertion: ConstraintType): ConstraintDescriptor => Boolean =
    assertion match {
      case NodePropertyExistence         => c => c.isNodePropertyExistenceConstraint
      case RelationshipPropertyExistence => c => c.isRelationshipPropertyExistenceConstraint
      case NodeUniqueness                => c => c.isNodeUniquenessConstraint
      case RelationshipUniqueness        => c => c.isRelationshipUniquenessConstraint
      case NodeKey                       => c => c.isNodeKeyConstraint
      case RelationshipKey               => c => c.isRelationshipKeyConstraint
      case NodePropertyType(propType) =>
        c => c.isNodePropertyTypeConstraint && checkTypes(propType, c.asPropertyTypeConstraint().propertyType())
      case RelationshipPropertyType(propType) =>
        c =>
          c.isRelationshipPropertyTypeConstraint &&
            checkTypes(propType, c.asPropertyTypeConstraint().propertyType())
    }

  // Checks if the pre-existing constraints property type (preExistingTypes)
  // is the same as the property type of the constraint to be created (askedForType)
  private def checkTypes(askedForType: CypherType, preExistingTypes: PropertyTypeSet): Boolean =
    preExistingTypes.equals(PropertyTypeMapper.asPropertyTypeSet(askedForType))

  implicit private def propertyToId(ctx: QueryContext)(property: PropertyKeyName): PropertyKeyId =
    PropertyKeyId(ctx.getOrCreatePropertyKeyId(property.name))

  private def labelPropWithName(ctx: QueryContext)(label: LabelName, prop: PropertyKeyName, name: Option[String]) =
    (ctx.getOrCreateLabelId(label.name), ctx.getOrCreatePropertyKeyId(prop.name), name)

  private def typePropWithName(ctx: QueryContext)(relType: RelTypeName, prop: PropertyKeyName, name: Option[String]) =
    (ctx.getOrCreateRelTypeId(relType.name), ctx.getOrCreatePropertyKeyId(prop.name), name)
}
