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
package org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands

import org.neo4j.common.EntityType
import org.neo4j.cypher.internal.ast.AllIndexes
import org.neo4j.cypher.internal.ast.CommandResultItem
import org.neo4j.cypher.internal.ast.FulltextIndexes
import org.neo4j.cypher.internal.ast.LookupIndexes
import org.neo4j.cypher.internal.ast.PointIndexes
import org.neo4j.cypher.internal.ast.RangeIndexes
import org.neo4j.cypher.internal.ast.ShowColumn
import org.neo4j.cypher.internal.ast.ShowIndexType
import org.neo4j.cypher.internal.ast.TextIndexes
import org.neo4j.cypher.internal.ast.VectorIndexes
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.IndexInfo
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowIndexesCommand.createIndexStatement
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.asEscapedProcedureArgumentString
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.asEscapedString
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.barStringJoiner
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.colonStringJoiner
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.configAsString
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.escapeBackticks
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.extractOptionsMap
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.optionsAsString
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.pointConfigValueAsString
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.propStringJoiner
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.relPropStringJoiner
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.internal.schema.ConstraintDescriptor
import org.neo4j.internal.schema.IndexConfig
import org.neo4j.internal.schema.IndexDescriptor
import org.neo4j.internal.schema.IndexType
import org.neo4j.kernel.api.impl.schema.vector.VectorUtils
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.StringValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues

import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters.SeqHasAsJava

// SHOW [ALL|FULLTEXT|LOOKUP|POINT|RANGE|TEXT|VECTOR] INDEX[ES] [BRIEF|VERBOSE|WHERE clause|YIELD clause]
case class ShowIndexesCommand(
  indexType: ShowIndexType,
  verbose: Boolean,
  columns: List[ShowColumn],
  yieldColumns: List[CommandResultItem]
) extends Command(columns, yieldColumns) {

  override def originalNameRows(state: QueryState, baseRow: CypherRow): ClosingIterator[Map[String, AnyValue]] = {
    val ctx = state.query
    ctx.assertShowIndexAllowed()
    val constraintIdToName = ctx.getAllConstraints()
      .map { case (descriptor, _) => descriptor.getId -> descriptor.getName }
    val indexes: Map[IndexDescriptor, IndexInfo] = ctx.getAllIndexes()
    val relevantIndexes = indexType match {
      case AllIndexes => indexes
      case RangeIndexes =>
        indexes.filter {
          case (indexDescriptor, _) => indexDescriptor.getIndexType.equals(IndexType.RANGE)
        }
      case FulltextIndexes =>
        indexes.filter {
          case (indexDescriptor, _) => indexDescriptor.getIndexType.equals(IndexType.FULLTEXT)
        }
      case TextIndexes =>
        indexes.filter {
          case (indexDescriptor, _) => indexDescriptor.getIndexType.equals(IndexType.TEXT)
        }
      case PointIndexes =>
        indexes.filter {
          case (indexDescriptor, _) => indexDescriptor.getIndexType.equals(IndexType.POINT)
        }
      case VectorIndexes =>
        indexes.filter {
          case (indexDescriptor, _) => indexDescriptor.getIndexType.equals(IndexType.VECTOR)
        }
      case LookupIndexes =>
        indexes.filter {
          case (indexDescriptor, _) => indexDescriptor.getIndexType.equals(IndexType.LOOKUP)
        }
    }

    val sortedRelevantIndexes: ListMap[IndexDescriptor, IndexInfo] =
      ListMap(relevantIndexes.toSeq.sortBy(_._1.getName): _*)

    val zoneId = getConfiguredTimeZone(ctx)
    def getAsTime(timeInMs: Long) =
      Values.temporalValue(formatTime(timeInMs, zoneId).toZonedDateTime)

    val rows = sortedRelevantIndexes.map {
      case (indexDescriptor: IndexDescriptor, indexInfo: IndexInfo) =>
        val indexStatus = indexInfo.indexStatus

        val maybeOwningConstraintId = indexDescriptor.getOwningConstraintId
        val owningConstraint =
          if (maybeOwningConstraintId.isPresent)
            constraintIdToName.get(maybeOwningConstraintId.getAsLong)
              .map(Values.stringValue)
              .getOrElse(Values.NO_VALUE)
          else Values.NO_VALUE

        val indexType = indexDescriptor.getIndexType
        val isLookupIndex = indexType.equals(IndexType.LOOKUP)

        val name = indexDescriptor.getName

        val entityType = indexDescriptor.schema.entityType
        val labelsOrTypes = indexInfo.labelsOrTypes
        val properties = indexInfo.properties
        val providerName = indexDescriptor.getIndexProvider.name
        val labelsOrTypesValue =
          if (isLookupIndex) Values.NO_VALUE
          else VirtualValues.fromList(labelsOrTypes.map(elem => Values.of(elem).asInstanceOf[AnyValue]).asJava)
        val propertiesValue =
          if (isLookupIndex) Values.NO_VALUE
          else VirtualValues.fromList(properties.map(prop => Values.of(prop).asInstanceOf[AnyValue]).asJava)

        val indexStatistics = ctx.getIndexUsageStatistics(indexDescriptor)
        val trackedSinceKernelValue = indexStatistics.trackedSince()
        val lastReadKernelValue = indexStatistics.lastRead()
        val readCountKernelValue = indexStatistics.readCount()

        // Interpreting the kernel values into what SHOW INDEXES should return
        val (lastRead, readCount, trackedSince) =
          if (trackedSinceKernelValue == 0) {
            // not tracked at all: all columns null
            (Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE)
          } else if (lastReadKernelValue == 0) {
            // tracked but not yet read:
            // trackedSince should have real value, others default for when tracked but not seen
            (Values.NO_VALUE, Values.longValue(0L), getAsTime(trackedSinceKernelValue))
          } else {
            // all columns have available values
            (getAsTime(lastReadKernelValue), Values.longValue(readCountKernelValue), getAsTime(trackedSinceKernelValue))
          }

        val briefResult = Map(
          // The id of the index
          "id" -> Values.longValue(indexDescriptor.getId),
          // Name of the index, for example "myIndex"
          "name" -> Values.stringValue(name),
          // Current state of the index, one of "ONLINE", "FAILED", "POPULATING"
          "state" -> Values.stringValue(indexStatus.state),
          // % of index population, for example 0.0, 100.0, or 75.1
          "populationPercent" -> Values.doubleValue(indexStatus.populationProgress),
          // The IndexType of this index, either "FULLTEXT", "TEXT", "RANGE", "POINT", "VECTOR" or "LOOKUP"
          "type" -> Values.stringValue(indexType.name),
          // Type of entities this index represents, either "NODE" or "RELATIONSHIP"
          "entityType" -> Values.stringValue(entityType.name),
          // The labels or relationship types of this constraint, for example ["Label1", "Label2"] or ["RelType1", "RelType2"], null for lookup indexes
          "labelsOrTypes" -> labelsOrTypesValue,
          // The properties of this constraint, for example ["propKey", "propKey2"], null for lookup indexes
          "properties" -> propertiesValue,
          // The index provider for this index, one of "fulltext-1.0", "range-1.0", "point-1.0", "text-1.0", "token-lookup-1.0"
          "indexProvider" -> Values.stringValue(providerName),
          // The name of the constraint associated to the index
          "owningConstraint" -> owningConstraint,
          // Last time the index was used for reading
          "lastRead" -> lastRead,
          // The number of read queries that have been issued to this index
          "readCount" -> readCount
        )
        if (verbose) {
          val indexConfig = indexDescriptor.getIndexConfig
          val optionsValue = extractOptionsMap(providerName, indexConfig)
          briefResult ++ Map(
            // The time when usage statistics tracking started for this index
            "trackedSince" -> trackedSince,
            "options" -> optionsValue,
            "failureMessage" -> Values.stringValue(indexStatus.failureMessage),
            "createStatement" -> Values.stringValue(
              createIndexStatement(
                name,
                indexType,
                entityType,
                labelsOrTypes,
                properties,
                providerName,
                indexConfig,
                indexStatus.maybeConstraint
              )
            )
          )
        } else {
          briefResult
        }
    }
    val updatedRows = updateRowsWithPotentiallyRenamedColumns(rows.toList)
    ClosingIterator.apply(updatedRows.iterator)
  }

}

object ShowIndexesCommand {
  sealed trait Uniqueness

  case object Unique extends Uniqueness {
    final override val toString: String = "UNIQUE"
  }

  case object Nonunique extends Uniqueness {
    final override val toString: String = "NONUNIQUE"
  }

  private def createIndexStatement(
    name: String,
    indexType: IndexType,
    entityType: EntityType,
    labelsOrTypes: List[String],
    properties: List[String],
    providerName: String,
    indexConfig: IndexConfig,
    maybeConstraint: Option[ConstraintDescriptor]
  ): String = {

    val escapedName = s"`${escapeBackticks(name)}`"

    def constraintCommand(
      nodeOrRelPattern: String,
      escapedProperties: String,
      predicate: String,
      options: String
    ): String =
      s"CREATE CONSTRAINT $escapedName FOR $nodeOrRelPattern REQUIRE ($escapedProperties) $predicate OPTIONS $options"

    indexType match {
      case IndexType.RANGE =>
        val labelsOrTypesWithColons = asEscapedString(labelsOrTypes, colonStringJoiner)

        maybeConstraint match {
          case Some(constraint) if constraint.isNodeUniquenessConstraint =>
            val escapedNodeProperties = asEscapedString(properties, propStringJoiner)
            val optionsString = s"{indexConfig: {}, indexProvider: '$providerName'}"
            constraintCommand(s"(n$labelsOrTypesWithColons)", escapedNodeProperties, "IS UNIQUE", optionsString)
          case Some(constraint) if constraint.isRelationshipUniquenessConstraint =>
            val escapedRelProperties = asEscapedString(properties, relPropStringJoiner)
            val optionsString = s"{indexConfig: {}, indexProvider: '$providerName'}"
            constraintCommand(s"()-[r$labelsOrTypesWithColons]-()", escapedRelProperties, "IS UNIQUE", optionsString)
          case Some(constraint) if constraint.isNodeKeyConstraint =>
            val escapedNodeProperties = asEscapedString(properties, propStringJoiner)
            val optionsString = s"{indexConfig: {}, indexProvider: '$providerName'}"
            constraintCommand(s"(n$labelsOrTypesWithColons)", escapedNodeProperties, "IS NODE KEY", optionsString)
          case Some(constraint) if constraint.isRelationshipKeyConstraint =>
            val escapedRelProperties = asEscapedString(properties, relPropStringJoiner)
            val optionsString = s"{indexConfig: {}, indexProvider: '$providerName'}"
            constraintCommand(
              s"()-[r$labelsOrTypesWithColons]-()",
              escapedRelProperties,
              "IS RELATIONSHIP KEY",
              optionsString
            )
          case Some(_) =>
            throw new IllegalArgumentException(
              "Expected an index or index backed constraint, found another constraint."
            )
          case None =>
            entityType match {
              case EntityType.NODE =>
                val escapedNodeProperties = asEscapedString(properties, propStringJoiner)
                s"CREATE RANGE INDEX $escapedName FOR (n$labelsOrTypesWithColons) ON ($escapedNodeProperties)"
              case EntityType.RELATIONSHIP =>
                val escapedRelProperties = asEscapedString(properties, relPropStringJoiner)
                s"CREATE RANGE INDEX $escapedName FOR ()-[r$labelsOrTypesWithColons]-() ON ($escapedRelProperties)"
              case _ => throw new IllegalArgumentException(s"Did not recognize entity type $entityType")
            }
        }
      case IndexType.FULLTEXT =>
        val labelsOrTypesWithBars = asEscapedString(labelsOrTypes, barStringJoiner)
        val fulltextConfig = configAsString(indexConfig, value => fullTextConfigValueAsString(value))
        val optionsString = optionsAsString(providerName, fulltextConfig)

        entityType match {
          case EntityType.NODE =>
            val escapedNodeProperties = asEscapedString(properties, propStringJoiner)
            s"CREATE FULLTEXT INDEX $escapedName FOR (n$labelsOrTypesWithBars) ON EACH [$escapedNodeProperties] OPTIONS $optionsString"
          case EntityType.RELATIONSHIP =>
            val escapedRelProperties = asEscapedString(properties, relPropStringJoiner)
            s"CREATE FULLTEXT INDEX $escapedName FOR ()-[r$labelsOrTypesWithBars]-() ON EACH [$escapedRelProperties] OPTIONS $optionsString"
          case _ => throw new IllegalArgumentException(s"Did not recognize entity type $entityType")
        }
      case IndexType.TEXT =>
        val labelsOrTypesWithColons = asEscapedString(labelsOrTypes, colonStringJoiner)
        val optionsString = s"{indexConfig: {}, indexProvider: '$providerName'}"

        entityType match {
          case EntityType.NODE =>
            val escapedNodeProperties = asEscapedString(properties, propStringJoiner)
            s"CREATE TEXT INDEX $escapedName FOR (n$labelsOrTypesWithColons) ON ($escapedNodeProperties) OPTIONS $optionsString"
          case EntityType.RELATIONSHIP =>
            val escapedRelProperties = asEscapedString(properties, relPropStringJoiner)
            s"CREATE TEXT INDEX $escapedName FOR ()-[r$labelsOrTypesWithColons]-() ON ($escapedRelProperties) OPTIONS $optionsString"
          case _ => throw new IllegalArgumentException(s"Did not recognize entity type $entityType")
        }
      case IndexType.POINT =>
        val labelsOrTypesWithColons = asEscapedString(labelsOrTypes, colonStringJoiner)
        val pointConfig = configAsString(indexConfig, value => pointConfigValueAsString(value))
        val optionsString = optionsAsString(providerName, pointConfig)

        entityType match {
          case EntityType.NODE =>
            val escapedNodeProperties = asEscapedString(properties, propStringJoiner)
            s"CREATE POINT INDEX $escapedName FOR (n$labelsOrTypesWithColons) ON ($escapedNodeProperties) OPTIONS $optionsString"
          case EntityType.RELATIONSHIP =>
            val escapedRelProperties = asEscapedString(properties, relPropStringJoiner)
            s"CREATE POINT INDEX $escapedName FOR ()-[r$labelsOrTypesWithColons]-() ON ($escapedRelProperties) OPTIONS $optionsString"
          case _ => throw new IllegalArgumentException(s"Did not recognize entity type $entityType")
        }
      case IndexType.VECTOR =>
        entityType match {
          case EntityType.NODE =>
            val escapedName = asEscapedProcedureArgumentString(name)
            val escapedLabel = asEscapedProcedureArgumentString(labelsOrTypes.head)
            val escapedPropertyKey = asEscapedProcedureArgumentString(properties.last)
            val dimension = VectorUtils.vectorDimensionsFrom(indexConfig)
            val escapedSimilarityFunction =
              asEscapedProcedureArgumentString(VectorUtils.vectorSimilarityFunctionFrom(indexConfig).name)
            s"CALL db.index.vector.createNodeIndex($escapedName, $escapedLabel, $escapedPropertyKey, $dimension, $escapedSimilarityFunction)"
          case EntityType.RELATIONSHIP =>
            throw new IllegalArgumentException(s"$entityType not valid for $indexType index")
          case _ => throw new IllegalArgumentException(s"Did not recognize entity type $entityType")
        }
      case IndexType.LOOKUP =>
        entityType match {
          case EntityType.NODE =>
            s"CREATE LOOKUP INDEX $escapedName FOR (n) ON EACH labels(n)"
          case EntityType.RELATIONSHIP =>
            s"CREATE LOOKUP INDEX $escapedName FOR ()-[r]-() ON EACH type(r)"
          case _ => throw new IllegalArgumentException(s"Did not recognize entity type $entityType")
        }
      case _ => throw new IllegalArgumentException(s"Did not recognize index type $indexType")
    }
  }

  private def fullTextConfigValueAsString(configValue: Value): String = {
    configValue match {
      case booleanValue: BooleanValue => booleanValue.booleanValue().toString
      case stringValue: StringValue   => "'" + stringValue.stringValue() + "'"
      case _ => throw new IllegalArgumentException(s"Could not convert config value '$configValue' to config string.")
    }
  }
}
