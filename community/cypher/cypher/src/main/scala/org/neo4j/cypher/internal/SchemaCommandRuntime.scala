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

import java.util.Collections
import java.util.StringJoiner

import org.neo4j.common.EntityType
import org.neo4j.cypher.internal.ast.AllConstraints
import org.neo4j.cypher.internal.ast.ExistsConstraints
import org.neo4j.cypher.internal.ast.NodeExistsConstraints
import org.neo4j.cypher.internal.ast.NodeKeyConstraints
import org.neo4j.cypher.internal.ast.RelExistsConstraints
import org.neo4j.cypher.internal.ast.ShowConstraintType
import org.neo4j.cypher.internal.ast.UniqueConstraints
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
import org.neo4j.cypher.internal.logical.plans.ShowConstraints
import org.neo4j.cypher.internal.logical.plans.ShowIndexes
import org.neo4j.cypher.internal.logical.plans.Uniqueness
import org.neo4j.cypher.internal.options.CypherRuntimeOption
import org.neo4j.cypher.internal.procs.AssertConstraint
import org.neo4j.cypher.internal.procs.AssertIndex
import org.neo4j.cypher.internal.procs.IgnoredResult
import org.neo4j.cypher.internal.procs.SchemaReadExecutionPlan
import org.neo4j.cypher.internal.procs.SchemaReadExecutionResult
import org.neo4j.cypher.internal.procs.SchemaWriteExecutionPlan
import org.neo4j.cypher.internal.procs.SuccessResult
import org.neo4j.cypher.internal.runtime.ConstraintInfo
import org.neo4j.cypher.internal.runtime.IndexInfo
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
import org.neo4j.internal.schema
import org.neo4j.internal.schema.ConstraintDescriptor
import org.neo4j.internal.schema.IndexConfig
import org.neo4j.internal.schema.IndexDescriptor
import org.neo4j.internal.schema.IndexType
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.kernel.impl.index.schema.FulltextIndexProviderFactory
import org.neo4j.kernel.impl.index.schema.GenericNativeIndexProvider
import org.neo4j.kernel.impl.index.schema.fusion.NativeLuceneFusionIndexProviderFactory30
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.DoubleArray
import org.neo4j.values.storable.IntValue
import org.neo4j.values.storable.StringValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.collection.immutable.ListMap
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
      SchemaWriteExecutionPlan("CreateNodeKeyConstraint", ctx => {
        val (indexProvider, indexConfig) = getValidProviderAndConfig(options, "node key constraint")
        val labelId = ctx.getOrCreateLabelId(label.name)
        val propertyKeyIds = props.map(p => propertyToId(ctx)(p.propertyKey).id)
        ctx.createNodeKeyConstraint(labelId, propertyKeyIds, name, indexProvider, indexConfig)
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

    // CREATE CONSTRAINT [name] [IF NOT EXISTS] ON (node:Label) ASSERT node.prop IS UNIQUE [OPTIONS {...}]
    // CREATE CONSTRAINT [name] [IF NOT EXISTS] ON (node:Label) ASSERT (node.prop1,node.prop2) IS UNIQUE [OPTIONS {...}]
    case CreateUniquePropertyConstraint(source, _, label, props, name, options) => (context, parameterMapping) =>
      SchemaWriteExecutionPlan("CreateUniqueConstraint", ctx => {
        val (indexProvider, indexConfig) = getValidProviderAndConfig(options, "uniqueness constraint")
        val labelId = ctx.getOrCreateLabelId(label.name)
        val propertyKeyIds = props.map(p => propertyToId(ctx)(p.propertyKey).id)
        ctx.createUniqueConstraint(labelId, propertyKeyIds, name, indexProvider, indexConfig)
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

    // CREATE CONSTRAINT [name] [IF NOT EXISTS] ON (node:Label) ASSERT node.prop IS NOT NULL
    case CreateNodePropertyExistenceConstraint(source, label, prop, name) => (context, parameterMapping) =>
      SchemaWriteExecutionPlan("CreateNodePropertyExistenceConstraint", ctx => {
        (ctx.createNodePropertyExistenceConstraint _).tupled(labelPropWithName(ctx)(label, prop.propertyKey, name))
        SuccessResult
      }, source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping)))

    // DROP CONSTRAINT ON (node:Label) ASSERT EXISTS (node.prop)
    case DropNodePropertyExistenceConstraint(label, prop) => (_, _) =>
      SchemaWriteExecutionPlan("DropNodePropertyExistenceConstraint", ctx => {
        (ctx.dropNodePropertyExistenceConstraint _).tupled(labelProp(ctx)(label, prop.propertyKey))
        SuccessResult
      })

    // CREATE CONSTRAINT [name] [IF NOT EXISTS] ON ()-[r:R]-() ASSERT r.prop IS NOT NULL
    case CreateRelationshipPropertyExistenceConstraint(source, relType, prop, name) => (context, parameterMapping) =>
      SchemaWriteExecutionPlan("CreateRelationshipPropertyExistenceConstraint", ctx => {
        (ctx.createRelationshipPropertyExistenceConstraint _).tupled(typePropWithName(ctx)(relType, prop.propertyKey, name))
        SuccessResult
      }, source.map(logicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping)))

    // DROP CONSTRAINT ON ()-[r:R]-() ASSERT EXISTS (r.prop)
    case DropRelationshipPropertyExistenceConstraint(relType, prop) => (_, _) =>
      SchemaWriteExecutionPlan("DropRelationshipPropertyExistenceConstraint", ctx => {
        (ctx.dropRelationshipPropertyExistenceConstraint _).tupled(typeProp(ctx)(relType, prop.propertyKey))
        SuccessResult
      })

    // DROP CONSTRAINT name [IF EXISTS]
    case DropConstraintOnName(name, ifExists) => (_, _) =>
      SchemaWriteExecutionPlan("DropConstraint", ctx => {
        if (!ifExists || ctx.constraintExists(name)) {
          ctx.dropNamedConstraint(name)
        }
        SuccessResult
      })

    // SHOW [ALL|UNIQUE|NODE EXIST[S]|RELATIONSHIP EXIST[S]|EXIST[S]|NODE KEY] CONSTRAINT[S] [BRIEF|VERBOSE[OUTPUT]]
    case ShowConstraints(constraintType, verbose, defaultColumnNames) => (_, _) =>
      SchemaReadExecutionPlan("ShowConstraints", AssertConstraint, ctx => {
        val constraints = ctx.getAllConstraints()

        val predicate: ConstraintDescriptor => Boolean = constraintType match {
          case UniqueConstraints => c => c.`type`().equals(schema.ConstraintType.UNIQUE)
          case NodeKeyConstraints => c => c.`type`().equals(schema.ConstraintType.UNIQUE_EXISTS)
          case ExistsConstraints => c => c.`type`().equals(schema.ConstraintType.EXISTS)
          case NodeExistsConstraints => c => c.`type`().equals(schema.ConstraintType.EXISTS) && c.schema.entityType.equals(EntityType.NODE)
          case RelExistsConstraints => c => c.`type`().equals(schema.ConstraintType.EXISTS) && c.schema.entityType.equals(EntityType.RELATIONSHIP)
          case AllConstraints => _ => true // Should keep all and not filter away any constraints
          case c => throw new IllegalStateException(s"Unknown constraint type: $c")
        }

        val relevantConstraints = constraints.filter {
          case (constraintDescriptor, _) => predicate(constraintDescriptor)
        }
        val sortedRelevantConstraints: ListMap[ConstraintDescriptor, ConstraintInfo] = ListMap(relevantConstraints.toSeq.sortBy(_._1.getName):_*)

        val result: List[Map[String, AnyValue]] = sortedRelevantConstraints.map {
          case (constraintDescriptor: ConstraintDescriptor, constraintInfo: ConstraintInfo) =>
            val escapedName = escapeBackticks(constraintDescriptor.getName)
            val labels = constraintInfo.labelsOrTypes
            val properties = constraintInfo.properties
            val isIndexBacked = constraintDescriptor.isIndexBackedConstraint
            val entityType = constraintDescriptor.schema.entityType
            val constraintType = getConstraintType(constraintDescriptor.`type`, entityType)

            val briefResult = Map(
              // The id of the constraint
              "id" -> Values.longValue(constraintDescriptor.getId),
              // Name of the constraint, for example "myConstraint"
              "name" -> Values.stringValue(escapedName),
              // The ConstraintType of this constraint, one of "UNIQUENESS", "NODE_KEY", "NODE_PROPERTY_EXISTENCE", "RELATIONSHIP_PROPERTY_EXISTENCE"
              "type" -> Values.stringValue(constraintType.output),
              // Type of entities this constraint represents, either "NODE" or "RELATIONSHIP"
              "entityType" -> Values.stringValue(entityType.name),
              // The labels or relationship types of this constraint, for example ["Label1", "Label2"] or ["RelType1", "RelType2"]
              "labelsOrTypes" -> VirtualValues.fromList(labels.map(elem => Values.of(elem).asInstanceOf[AnyValue]).asJava),
              // The properties of this constraint, for example ["propKey", "propKey2"]
              "properties" -> VirtualValues.fromList(properties.map(prop => Values.of(prop).asInstanceOf[AnyValue]).asJava),
              // The id of the index associated to the constraint
              "ownedIndexId" -> {if (isIndexBacked) Values.longValue(constraintDescriptor.asIndexBackedConstraint().ownedIndexId()) else Values.NO_VALUE}
            )
            if (verbose) {
              val (options, createString) = if (isIndexBacked) {
                val index = constraintInfo.maybeIndex.getOrElse(
                  throw new IllegalStateException(s"Expected to find an index for index backed constraint $escapedName")
                )
                val providerName = index.getIndexProvider.name
                val indexConfig = index.getIndexConfig
                val options: MapValue = extractOptionsMap(providerName, indexConfig)
                val createWithOptions = createConstraintStatement(escapedName, constraintType, labels, properties, Some(providerName), Some(indexConfig))
                (options, createWithOptions)
              } else {
                val createWithoutOptions = createConstraintStatement(escapedName, constraintType, labels, properties)
                (Values.NO_VALUE, createWithoutOptions)
              }

              briefResult ++ Map(
                "options" -> options,
                "createStatement" -> Values.stringValue(createString)
              )
            } else {
              briefResult
            }
        }.toList
        SchemaReadExecutionResult(defaultColumnNames.toArray, result)
      })

    // CREATE INDEX ON :LABEL(prop)
    // CREATE INDEX [name] [IF NOT EXISTS] FOR (n:LABEL) ON (n.prop) [OPTIONS {...}]
    case CreateIndex(source, label, props, name, options) => (context, parameterMapping) =>
      SchemaWriteExecutionPlan("CreateIndex", ctx => {
        val (indexProvider, indexConfig) = getValidProviderAndConfig(options, "index")
        val labelId = ctx.getOrCreateLabelId(label.name)
        val propertyKeyIds = props.map(p => propertyToId(ctx)(p).id)
        ctx.addIndexRule(labelId, propertyKeyIds, name, indexProvider, indexConfig)
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

    // DROP INDEX name [IF EXISTS]
    case DropIndexOnName(name, ifExists) => (_, _) =>
      SchemaWriteExecutionPlan("DropIndex", ctx => {
        if (!ifExists || ctx.indexExists(name)) {
          ctx.dropIndexRule(name)
        }
        SuccessResult
      })

    // SHOW [ALL|BTREE] INDEX[ES] [BRIEF|VERBOSE[OUTPUT]]
    case ShowIndexes(all, verbose, defaultColumnNames) => (_, _) =>
      SchemaReadExecutionPlan("ShowIndexes", AssertIndex, ctx => {
        val indexes: Map[IndexDescriptor, IndexInfo] = ctx.getAllIndexes()
        val relevantIndexes = if (all) indexes else indexes.filter {
          case (indexDescriptor, _) => indexDescriptor.getIndexType.equals(IndexType.BTREE)
        }
        val sortedRelevantIndexes: ListMap[IndexDescriptor, IndexInfo] = ListMap(relevantIndexes.toSeq.sortBy(_._1.getName):_*)
        val result: List[Map[String, AnyValue]] = sortedRelevantIndexes.map {
          case (indexDescriptor: IndexDescriptor, indexInfo: IndexInfo) =>
            val indexStatus = indexInfo.indexStatus
            val uniqueness = if (indexDescriptor.isUnique) Unique.toString else Nonunique.toString
            val indexType = indexDescriptor.getIndexType

            /*
             * For btree the create command is the Cypher CREATE INDEX which needs the name to be escaped,
             * in case it contains special characters.
             * For fulltext the create command is a procedure which doesn't require escaping.
            */
            val name = indexDescriptor.getName
            val escapedName = escapeBackticks(name)
            val createName = if(indexType.equals(IndexType.BTREE)) s"`$escapedName`" else name

            val entityType = indexDescriptor.schema.entityType
            val labels = indexInfo.labelsOrTypes
            val properties = indexInfo.properties
            val providerName = indexDescriptor.getIndexProvider.name

            val briefResult = Map(
              // The id of the index
              "id" -> Values.longValue(indexDescriptor.getId),
              // Name of the index, for example "myIndex"
              "name" -> Values.stringValue(escapedName),
              // Current state of the index, one of "ONLINE", "FAILED", "POPULATING"
              "state" -> Values.stringValue(indexStatus.state),
              // % of index population, for example 0.0, 100.0, or 75.1
              "populationPercent" -> Values.doubleValue(indexStatus.populationProgress),
              // Tells if the index is only meant to allow one value per key, either "UNIQUE" or "NONUNIQUE"
              "uniqueness" -> Values.stringValue(uniqueness),
              // The IndexType of this index, either "FULLTEXT" or "BTREE"
              "type" -> Values.stringValue(indexType.name),
              // Type of entities this index represents, either "NODE" or "RELATIONSHIP"
              "entityType" -> Values.stringValue(entityType.name),
              // The labels or relationship types of this constraint, for example ["Label1", "Label2"] or ["RelType1", "RelType2"]
              "labelsOrTypes" -> VirtualValues.fromList(labels.map(elem => Values.of(elem).asInstanceOf[AnyValue]).asJava),
              // The properties of this constraint, for example ["propKey", "propKey2"]
              "properties" -> VirtualValues.fromList(properties.map(prop => Values.of(prop).asInstanceOf[AnyValue]).asJava),
              // The index provider for this index, one of "native-btree-1.0", "lucene+native-3.0", "fulltext-1.0"
              "indexProvider" -> Values.stringValue(providerName)
            )
            if (verbose) {
              val indexConfig = indexDescriptor.getIndexConfig
              briefResult ++ Map(
                "options" -> extractOptionsMap(providerName, indexConfig),
                "failureMessage" -> Values.stringValue(indexInfo.indexStatus.failureMessage),
                "createStatement" -> Values.stringValue(
                  createIndexStatement(createName, indexType, entityType, labels, properties, providerName, indexConfig, indexStatus.maybeConstraint))
              )
            } else {
              briefResult
            }
        }.toList
        SchemaReadExecutionResult(defaultColumnNames.toArray, result)
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

  private def createIndexStatement(escapedName: String,
                              indexType: IndexType,
                              entityType: EntityType,
                              labelsOrTypes: List[String],
                              properties: List[String],
                              providerName: String,
                              indexConfig: IndexConfig,
                              maybeConstraint: Option[ConstraintDescriptor]): String = {

    indexType match {
      case IndexType.BTREE =>
        val labelsOrTypesWithColons = asEscapedString(labelsOrTypes, colonStringJoiner)
        val escapedProperties = asEscapedString(properties, propStringJoiner)
        val btreeConfig = configAsString(indexConfig, value => btreeConfigValueAsString(value))
        val optionsString = optionsAsString(providerName, btreeConfig)

        maybeConstraint match {
          case Some(constraint) if constraint.isUniquenessConstraint =>
            s"CREATE CONSTRAINT $escapedName ON (n$labelsOrTypesWithColons) ASSERT ($escapedProperties) IS UNIQUE OPTIONS $optionsString"
          case Some(constraint) if constraint.isNodeKeyConstraint =>
            s"CREATE CONSTRAINT $escapedName ON (n$labelsOrTypesWithColons) ASSERT ($escapedProperties) IS NODE KEY OPTIONS $optionsString"
          case Some(_) =>
            throw new IllegalArgumentException("Expected an index or index backed constraint, found another constraint.")
          case None =>
            s"CREATE INDEX $escapedName FOR (n$labelsOrTypesWithColons) ON ($escapedProperties) OPTIONS $optionsString"
        }
      case IndexType.FULLTEXT =>
        val labelsOrTypesArray = asString(labelsOrTypes, arrayStringJoiner)
        val propertiesArray = asString(properties, arrayStringJoiner)
        val fulltextConfig = configAsString(indexConfig, value => fullTextConfigValueAsString(value))

        entityType match {
          case EntityType.NODE =>
            s"CALL db.index.fulltext.createNodeIndex('$escapedName', $labelsOrTypesArray, $propertiesArray, $fulltextConfig)"
          case EntityType.RELATIONSHIP =>
            s"CALL db.index.fulltext.createRelationshipIndex('$escapedName', $labelsOrTypesArray, $propertiesArray, $fulltextConfig)"
          case _ => throw new IllegalArgumentException(s"Did not recognize entity type $entityType")
        }
      case _ => throw new IllegalArgumentException(s"Did not recognize index type $indexType")
    }
  }

  private def createConstraintStatement(name: String,
                                        constraintType: ShowConstraintType,
                                        labelsOrTypes: List[String],
                                        properties: List[String],
                                        providerName: Option[String] = None,
                                        indexConfig: Option[IndexConfig] = None): String = {
    val labelsOrTypesWithColons = asEscapedString(labelsOrTypes, colonStringJoiner)
    constraintType match {
      case UniqueConstraints =>
        val escapedProperties = asEscapedString(properties, propStringJoiner)
        val options = extractOptionsString(providerName, indexConfig, UniqueConstraints.prettyPrint)
        s"CREATE CONSTRAINT `$name` ON (n$labelsOrTypesWithColons) ASSERT ($escapedProperties) IS UNIQUE OPTIONS $options"
      case NodeKeyConstraints =>
        val escapedProperties = asEscapedString(properties, propStringJoiner)
        val options = extractOptionsString(providerName, indexConfig, NodeKeyConstraints.prettyPrint)
        s"CREATE CONSTRAINT `$name` ON (n$labelsOrTypesWithColons) ASSERT ($escapedProperties) IS NODE KEY OPTIONS $options"
      case NodeExistsConstraints =>
        val escapedProperties = asEscapedString(properties, propStringJoiner)
        s"CREATE CONSTRAINT `$name` ON (n$labelsOrTypesWithColons) ASSERT ($escapedProperties) IS NOT NULL"
      case RelExistsConstraints =>
        val escapedProperties = asEscapedString(properties, relPropStringJoiner)
        s"CREATE CONSTRAINT `$name` ON ()-[r$labelsOrTypesWithColons]-() ASSERT ($escapedProperties) IS NOT NULL"
      case _ => throw new IllegalArgumentException(s"Did not expect constraint type ${constraintType.prettyPrint} for constraint create command.")
    }
  }

  private def extractOptionsString(maybeProviderName: Option[String], maybeIndexConfig: Option[IndexConfig], constraintType: String): String = {
    val providerName = maybeProviderName.getOrElse(throw new IllegalArgumentException(s"Expected a provider name for $constraintType constraint."))
    val indexConfig = maybeIndexConfig.getOrElse(throw new IllegalArgumentException(s"Expected an index configuration for $constraintType constraint."))
    val btreeConfig = configAsString(indexConfig, value => btreeConfigValueAsString(value))
    optionsAsString(providerName, btreeConfig)
  }

  private def extractOptionsMap(providerName: String, indexConfig: IndexConfig): MapValue = {
    val (configKeys, configValues) = indexConfig.asMap().asScala.toSeq.unzip
    val optionKeys = Array("indexConfig", "indexProvider")
    val optionValues = Array(VirtualValues.map(configKeys.toArray, configValues.toArray), Values.stringValue(providerName))
    VirtualValues.map(optionKeys, optionValues)
  }

  private def asEscapedString(list: List[String], stringJoiner: StringJoiner): String = {
    for (elem <- list) {
      stringJoiner.add(s"`${escapeBackticks(elem)}`")
    }
    stringJoiner.toString
  }

  private def asString(list: List[String], stringJoiner: StringJoiner): String = {
    for (elem <- list) {
      stringJoiner.add(s"'$elem'")
    }
    stringJoiner.toString
  }

  private def optionsAsString(providerString: String, configString: String): String = {
    s"{indexConfig: $configString, indexProvider: '$providerString'}"
  }

  private def configAsString(indexConfig: IndexConfig, configValueAsString: Value => String): String = {
    val configString: StringJoiner = configStringJoiner
    val sortedIndexConfig = ListMap(indexConfig.asMap().asScala.toSeq.sortBy(_._1): _*)

    sortedIndexConfig.foldLeft(configString) { (acc, entry) =>
      val singleConfig: String = s"`${entry._1}`: ${configValueAsString(entry._2)}"
      acc.add(singleConfig)
    }
    configString.toString
  }

  private def btreeConfigValueAsString(configValue: Value): String = {
    configValue match {
      case doubleArray: DoubleArray => java.util.Arrays.toString(doubleArray.asObjectCopy);
      case intValue: IntValue => "" + intValue.value()
      case booleanValue: BooleanValue => "" + booleanValue.booleanValue()
      case stringValue: StringValue => "'" + stringValue.stringValue() + "'"
      case _ => throw new IllegalArgumentException(s"Could not convert config value '$configValue' to config string.")
    }
  }

  private def fullTextConfigValueAsString(configValue: Value): String = {
    configValue match {
      case booleanValue: BooleanValue => "'" + booleanValue.booleanValue() + "'"
      case stringValue: StringValue =>"'" + stringValue.stringValue() + "'"
      case _ => throw new IllegalArgumentException(s"Could not convert config value '$configValue' to config string.")
    }
  }

  private def colonStringJoiner = new StringJoiner(":",":", "")
  private def propStringJoiner = new StringJoiner(", n.","n.", "")
  private def relPropStringJoiner = new StringJoiner(", r.","r.", "")
  private def arrayStringJoiner = new StringJoiner(", ", "[", "]")
  private def configStringJoiner = new StringJoiner(",", "{", "}")

  sealed trait Uniqueness

  case object Unique extends Uniqueness {
    override final val toString: String = "UNIQUE"
  }

  case object Nonunique extends Uniqueness {
    override final val toString: String = "NONUNIQUE"
  }

  private def getConstraintType(internalConstraintType: schema.ConstraintType, entityType: EntityType): ShowConstraintType = {
    (internalConstraintType, entityType) match {
      case (schema.ConstraintType.UNIQUE, EntityType.NODE) => UniqueConstraints
      case (schema.ConstraintType.UNIQUE_EXISTS, EntityType.NODE) => NodeKeyConstraints
      case (schema.ConstraintType.EXISTS, EntityType.NODE) => NodeExistsConstraints
      case (schema.ConstraintType.EXISTS, EntityType.RELATIONSHIP) => RelExistsConstraints
      case _ => throw new IllegalStateException(s"Invalid constraint combination: ConstraintType $internalConstraintType and EntityType $entityType.")
    }
  }

  private def escapeBackticks(str: String): String = str.replaceAll("`", "``")
}
