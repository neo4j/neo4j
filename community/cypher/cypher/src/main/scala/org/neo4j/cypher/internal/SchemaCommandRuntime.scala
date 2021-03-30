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

import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.expressions.DoubleLiteral
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.logical.plans.ConstraintType
import org.neo4j.cypher.internal.logical.plans.CreateBtreeIndex
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
import org.neo4j.cypher.internal.options.CypherRuntimeOption
import org.neo4j.cypher.internal.procs.IgnoredResult
import org.neo4j.cypher.internal.procs.SchemaExecutionPlan
import org.neo4j.cypher.internal.procs.SuccessResult
import org.neo4j.cypher.internal.runtime.InternalQueryType
import org.neo4j.cypher.internal.runtime.ParameterMapping
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.SCHEMA_WRITE
import org.neo4j.cypher.internal.runtime.slottedParameters
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.graphdb.schema.IndexSettingImpl.FULLTEXT_ANALYZER
import org.neo4j.graphdb.schema.IndexSettingImpl.FULLTEXT_EVENTUALLY_CONSISTENT
import org.neo4j.graphdb.schema.IndexSettingUtil
import org.neo4j.internal.schema.ConstraintDescriptor
import org.neo4j.internal.schema.IndexConfig
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.kernel.impl.index.schema.FulltextIndexProviderFactory
import org.neo4j.kernel.impl.index.schema.GenericNativeIndexProvider
import org.neo4j.kernel.impl.index.schema.fusion.NativeLuceneFusionIndexProviderFactory30

import java.util.Collections
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.util.Try

/**
 * This runtime takes on queries that require no planning such as schema commands
 */
object SchemaCommandRuntime extends CypherRuntime[RuntimeContext] {
  override def name: String = "schema"

  override def correspondingRuntimeOption: Option[CypherRuntimeOption] = None

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
    // CREATE CONSTRAINT [name] [IF NOT EXISTS] ON (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY [OPTIONS {...}]
    case CreateNodeKeyConstraint(source, _, label, props, name, options) => (context, parameterMapping) =>
      SchemaExecutionPlan("CreateNodeKeyConstraint", ctx => {
        val (indexProvider, indexConfig) = getValidProviderAndConfig(options, "node key constraint")
        val labelId = ctx.getOrCreateLabelId(label.name)
        val propertyKeyIds = props.map(p => propertyToId(ctx)(p.propertyKey).id)
        ctx.createNodeKeyConstraint(labelId, propertyKeyIds, name, indexProvider, indexConfig)
        SuccessResult
      }, source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping)))

    // DROP CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY
    case DropNodeKeyConstraint(label, props) => (_, _) =>
      SchemaExecutionPlan("DropNodeKeyConstraint", ctx => {
        val labelId = ctx.getOrCreateLabelId(label.name)
        val propertyKeyIds = props.map(p => propertyToId(ctx)(p.propertyKey).id)
        ctx.dropNodeKeyConstraint(labelId, propertyKeyIds)
        SuccessResult
      })

    // CREATE CONSTRAINT [name] [IF NOT EXISTS] ON (node:Label) ASSERT node.prop IS UNIQUE [OPTIONS {...}]
    // CREATE CONSTRAINT [name] [IF NOT EXISTS] ON (node:Label) ASSERT (node.prop1,node.prop2) IS UNIQUE [OPTIONS {...}]
    case CreateUniquePropertyConstraint(source, _, label, props, name, options) => (context, parameterMapping) =>
      SchemaExecutionPlan("CreateUniqueConstraint", ctx => {
        val (indexProvider, indexConfig) = getValidProviderAndConfig(options, "uniqueness constraint")
        val labelId = ctx.getOrCreateLabelId(label.name)
        val propertyKeyIds = props.map(p => propertyToId(ctx)(p.propertyKey).id)
        ctx.createUniqueConstraint(labelId, propertyKeyIds, name, indexProvider, indexConfig)
        SuccessResult
      }, source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping)))

    // DROP CONSTRAINT ON (node:Label) ASSERT node.prop IS UNIQUE
    // DROP CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS UNIQUE
    case DropUniquePropertyConstraint(label, props) => (_, _) =>
      SchemaExecutionPlan("DropUniqueConstraint", ctx => {
        val labelId = ctx.getOrCreateLabelId(label.name)
        val propertyKeyIds = props.map(p => propertyToId(ctx)(p.propertyKey).id)
        ctx.dropUniqueConstraint(labelId, propertyKeyIds)
        SuccessResult
      })

    // CREATE CONSTRAINT [name] [IF NOT EXISTS] ON (node:Label) ASSERT node.prop IS NOT NULL
    case CreateNodePropertyExistenceConstraint(source, label, prop, name) => (context, parameterMapping) =>
      SchemaExecutionPlan("CreateNodePropertyExistenceConstraint", ctx => {
        (ctx.createNodePropertyExistenceConstraint _).tupled(labelPropWithName(ctx)(label, prop.propertyKey, name))
        SuccessResult
      }, source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping)))

    // DROP CONSTRAINT ON (node:Label) ASSERT EXISTS (node.prop)
    case DropNodePropertyExistenceConstraint(label, prop) => (_, _) =>
      SchemaExecutionPlan("DropNodePropertyExistenceConstraint", ctx => {
        (ctx.dropNodePropertyExistenceConstraint _).tupled(labelProp(ctx)(label, prop.propertyKey))
        SuccessResult
      })

    // CREATE CONSTRAINT [name] [IF NOT EXISTS] ON ()-[r:R]-() ASSERT r.prop IS NOT NULL
    case CreateRelationshipPropertyExistenceConstraint(source, relType, prop, name) => (context, parameterMapping) =>
      SchemaExecutionPlan("CreateRelationshipPropertyExistenceConstraint", ctx => {
        (ctx.createRelationshipPropertyExistenceConstraint _).tupled(typePropWithName(ctx)(relType, prop.propertyKey, name))
        SuccessResult
      }, source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping)))

    // DROP CONSTRAINT ON ()-[r:R]-() ASSERT EXISTS (r.prop)
    case DropRelationshipPropertyExistenceConstraint(relType, prop) => (_, _) =>
      SchemaExecutionPlan("DropRelationshipPropertyExistenceConstraint", ctx => {
        (ctx.dropRelationshipPropertyExistenceConstraint _).tupled(typeProp(ctx)(relType, prop.propertyKey))
        SuccessResult
      })

    // DROP CONSTRAINT name [IF EXISTS]
    case DropConstraintOnName(name, ifExists) => (_, _) =>
      SchemaExecutionPlan("DropConstraint", ctx => {
        if (!ifExists || ctx.constraintExists(name)) {
          ctx.dropNamedConstraint(name)
        }
        SuccessResult
      })

    // CREATE INDEX ON :LABEL(prop)
    // CREATE INDEX [name] [IF NOT EXISTS] FOR (n:LABEL) ON (n.prop) [OPTIONS {...}]
    // CREATE INDEX [name] [IF NOT EXISTS] FOR ()-[n:TYPE]-() ON (n.prop) [OPTIONS {...}]
    case CreateBtreeIndex(source, entityName, props, name, options) => (context, parameterMapping) =>
      SchemaExecutionPlan("CreateIndex", ctx => {
        val (indexProvider, indexConfig) = getValidProviderAndConfig(options, "index")
        val (entityId, isNodeIndex) = getEntityInfo(entityName, ctx)
        val propertyKeyIds = props.map(p => propertyToId(ctx)(p).id)
        ctx.addIndexRule(entityId, isNodeIndex, propertyKeyIds, name, indexProvider, indexConfig)
        SuccessResult
      }, source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping)))

    // DROP INDEX ON :LABEL(prop)
    case DropIndex(label, props) => (_, _) =>
      SchemaExecutionPlan("DropIndex", ctx => {
        val labelId = labelToId(ctx)(label)
        val propertyKeyIds = props.map(p => propertyToId(ctx)(p).id)
        ctx.dropIndexRule(labelId, propertyKeyIds)
        SuccessResult
      })

    // DROP INDEX name [IF EXISTS]
    case DropIndexOnName(name, ifExists) => (_, _) =>
      SchemaExecutionPlan("DropIndex", ctx => {
        if (!ifExists || ctx.indexExists(name)) {
          ctx.dropIndexRule(name)
        }
        SuccessResult
      })

    case DoNothingIfExistsForIndex(entityName, propertyKeyNames, name) => (_, _) =>
      SchemaExecutionPlan("DoNothingIfExist", ctx => {
        val (entityId, isNodeIndex) = getEntityInfo(entityName, ctx)
        val propertyKeyIds = propertyKeyNames.map(p => propertyToId(ctx)(p).id)
        if (Try(ctx.indexReference(entityId, isNodeIndex, propertyKeyIds: _*).getName).isSuccess) {
          IgnoredResult
        } else if (name.exists(ctx.indexExists)) {
          IgnoredResult
        } else {
          SuccessResult
        }
      }, None)

    case DoNothingIfExistsForConstraint(_, entityName, props, assertion, name) => (_, _) =>
      SchemaExecutionPlan("DoNothingIfExist", ctx => {
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
    // returns (entityId, isNodeEntity)
    case Left(label)    => (ctx.getOrCreateLabelId(label.name), true)
    case Right(relType) => (ctx.getOrCreateRelTypeId(relType.name), false)
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

  private def getValidProviderAndConfig(options: Map[String, Expression], schemaType: String): (Option[String], IndexConfig) = {
    val lowerCaseOptions = options.map { case (k, v) => (k.toLowerCase, v) }
    val maybeIndexProvider = lowerCaseOptions.get("indexprovider")
    val maybeConfig = lowerCaseOptions.get("indexconfig")

    val indexProvider = if (maybeIndexProvider.isDefined) Some(assertValidIndexProvider(maybeIndexProvider.get, schemaType)) else None
    val configMap: java.util.Map[String, Object] = if (maybeConfig.isDefined) assertValidAndTransformConfig(maybeConfig.get, schemaType) else Collections.emptyMap()
    val indexConfig = IndexSettingUtil.toIndexConfigFromStringObjectMap(configMap)

    (indexProvider, indexConfig)
  }

  private def assertValidIndexProvider(indexProvider: Expression, schemaType: String): String = indexProvider match {
    case s: StringLiteral =>
      val indexProviderValue = s.value

      if (indexProviderValue.equalsIgnoreCase(FulltextIndexProviderFactory.DESCRIPTOR.name()))
        throw new InvalidArgumentsException(
          s"""Could not create $schemaType with specified index provider '$indexProviderValue'.
             |To create fulltext index, please use 'db.index.fulltext.createNodeIndex' or 'db.index.fulltext.createRelationshipIndex'.""".stripMargin)

      if (!indexProviderValue.equalsIgnoreCase(GenericNativeIndexProvider.DESCRIPTOR.name()) &&
        !indexProviderValue.equalsIgnoreCase(NativeLuceneFusionIndexProviderFactory30.DESCRIPTOR.name()))
        throw new InvalidArgumentsException(s"Could not create $schemaType with specified index provider '$indexProviderValue'.")

      indexProviderValue

    case _ =>
      throw new InvalidArgumentsException(s"Could not create $schemaType with specified index provider '${indexProvider.asCanonicalStringVal}'. Expected String value.")
  }

  private def assertValidAndTransformConfig(config: Expression, schemaType: String): java.util.Map[String, Object] = {
    val exceptionWrongType =
      new InvalidArgumentsException(s"Could not create $schemaType with specified index config '${config.asCanonicalStringVal}'. Expected a map from String to Double[].")

    // for indexProvider BTREE:
    //    current keys: spatial.* (cartesian.|cartesian-3d.|wgs-84.|wgs-84-3d.) + (min|max)
    //    current values: Double[]
    config match {
      case MapExpression(items) =>
        if (items.exists { case (p, _) => p.name.equalsIgnoreCase(FULLTEXT_ANALYZER.getSettingName) || p.name.equalsIgnoreCase(FULLTEXT_EVENTUALLY_CONSISTENT.getSettingName) })
          throw new InvalidArgumentsException(
            s"""Could not create $schemaType with specified index config '${config.asCanonicalStringVal}', contains fulltext config options.
               |To create fulltext index, please use 'db.index.fulltext.createNodeIndex' or 'db.index.fulltext.createRelationshipIndex'.""".stripMargin)

        items.map {
        case (p, e: ListLiteral) =>
          val configValue: Object = e.expressions.map {
            case d: DoubleLiteral => d.value
            case _ => throw exceptionWrongType
          }.toArray
          (p.name, configValue)
        case _ => throw exceptionWrongType
      }.toMap.asJava
      case _ =>
        throw exceptionWrongType
    }
  }
}
