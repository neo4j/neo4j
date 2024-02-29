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
import org.neo4j.cypher.internal.ast.ShowIndexesClause.createStatementColumn
import org.neo4j.cypher.internal.ast.ShowIndexesClause.entityTypeColumn
import org.neo4j.cypher.internal.ast.ShowIndexesClause.failureMessageColumn
import org.neo4j.cypher.internal.ast.ShowIndexesClause.idColumn
import org.neo4j.cypher.internal.ast.ShowIndexesClause.indexProviderColumn
import org.neo4j.cypher.internal.ast.ShowIndexesClause.labelsOrTypesColumn
import org.neo4j.cypher.internal.ast.ShowIndexesClause.lastReadColumn
import org.neo4j.cypher.internal.ast.ShowIndexesClause.nameColumn
import org.neo4j.cypher.internal.ast.ShowIndexesClause.optionsColumn
import org.neo4j.cypher.internal.ast.ShowIndexesClause.owningConstraintColumn
import org.neo4j.cypher.internal.ast.ShowIndexesClause.populationPercentColumn
import org.neo4j.cypher.internal.ast.ShowIndexesClause.propertiesColumn
import org.neo4j.cypher.internal.ast.ShowIndexesClause.readCountColumn
import org.neo4j.cypher.internal.ast.ShowIndexesClause.stateColumn
import org.neo4j.cypher.internal.ast.ShowIndexesClause.trackedSinceColumn
import org.neo4j.cypher.internal.ast.ShowIndexesClause.typeColumn
import org.neo4j.cypher.internal.ast.TextIndexes
import org.neo4j.cypher.internal.ast.VectorIndexes
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.IndexInfo
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowIndexesCommand.createIndexStatement
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.asEscapedString
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.barStringJoiner
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.configAsString
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.createIndexCommand
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.createNodeConstraintCommand
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.createNodeIndexCommand
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.createRelConstraintCommand
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.createRelIndexCommand
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
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.IntValue
import org.neo4j.values.storable.LongValue
import org.neo4j.values.storable.StringValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues

import java.time.ZoneId

import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters.SeqHasAsJava

// SHOW [ALL|FULLTEXT|LOOKUP|POINT|RANGE|TEXT|VECTOR] INDEX[ES] [BRIEF|VERBOSE|WHERE clause|YIELD clause]
case class ShowIndexesCommand(
  indexType: ShowIndexType,
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
    val rows = sortedRelevantIndexes.map {
      case (indexDescriptor: IndexDescriptor, indexInfo: IndexInfo) =>
        // These don't really have a default/fallback and is used in multiple columns
        // so let's keep them as is regardless of if they are actually needed or not
        val indexType = indexDescriptor.getIndexType
        val isLookupIndex = indexType.equals(IndexType.LOOKUP)
        val entityType = indexDescriptor.schema.entityType
        val providerName = indexDescriptor.getIndexProvider.name

        val (lastRead, readCount, trackedSince) = getIndexStatistics(ctx, indexDescriptor, zoneId)

        requestedColumnsNames.map {
          // The id of the index
          case `idColumn` => idColumn -> Values.longValue(indexDescriptor.getId)
          // Name of the index, for example "myIndex"
          case `nameColumn` => nameColumn -> Values.stringValue(indexDescriptor.getName)
          // Current state of the index, one of "ONLINE", "FAILED", "POPULATING"
          case `stateColumn` => stateColumn -> Values.stringValue(indexInfo.indexStatus.state)
          // % of index population, for example 0.0, 100.0, or 75.1
          case `populationPercentColumn` =>
            populationPercentColumn -> Values.doubleValue(indexInfo.indexStatus.populationProgress)
          // The IndexType of this index, either "FULLTEXT", "TEXT", "RANGE", "POINT", "VECTOR" or "LOOKUP"
          case `typeColumn` => typeColumn -> Values.stringValue(indexType.name)
          // Type of entities this index represents, either "NODE" or "RELATIONSHIP"
          case `entityTypeColumn` => entityTypeColumn -> Values.stringValue(entityType.name)
          // The labels or relationship types of this constraint, for example ["Label1", "Label2"] or ["RelType1", "RelType2"], null for lookup indexes
          case `labelsOrTypesColumn` =>
            val labelsOrTypesValue =
              if (isLookupIndex) Values.NO_VALUE
              else VirtualValues.fromList(
                indexInfo.labelsOrTypes.map(elem => Values.of(elem).asInstanceOf[AnyValue]).asJava
              )
            labelsOrTypesColumn -> labelsOrTypesValue
          // The properties of this constraint, for example ["propKey", "propKey2"], null for lookup indexes
          case `propertiesColumn` =>
            val propertiesValue =
              if (isLookupIndex) Values.NO_VALUE
              else
                VirtualValues.fromList(indexInfo.properties.map(prop => Values.of(prop).asInstanceOf[AnyValue]).asJava)
            propertiesColumn -> propertiesValue
          // The index provider for this index, one of "fulltext-1.0", "range-1.0", "point-1.0", "text-1.0", "token-lookup-1.0"
          case `indexProviderColumn` => indexProviderColumn -> Values.stringValue(providerName)
          // The name of the constraint associated to the index
          case `owningConstraintColumn` =>
            val maybeOwningConstraintId = indexDescriptor.getOwningConstraintId
            val owningConstraint =
              if (maybeOwningConstraintId.isPresent)
                constraintIdToName.get(maybeOwningConstraintId.getAsLong)
                  .map(Values.stringValue)
                  .getOrElse(Values.NO_VALUE)
              else Values.NO_VALUE
            owningConstraintColumn -> owningConstraint
          // Last time the index was used for reading
          case `lastReadColumn` => lastReadColumn -> lastRead
          // The number of read queries that have been issued to this index
          case `readCountColumn` => readCountColumn -> readCount
          // The time when usage statistics tracking started for this index
          case `trackedSinceColumn` => trackedSinceColumn -> trackedSince
          // The options for this index, shows index provider and config
          case `optionsColumn` =>
            optionsColumn -> extractOptionsMap(providerName, indexDescriptor.getIndexConfig)
          // Message of failure should the index be in a failed state
          case `failureMessageColumn` =>
            failureMessageColumn -> Values.stringValue(indexInfo.indexStatus.failureMessage)
          // The statement to recreate the index
          case `createStatementColumn` =>
            createStatementColumn -> Values.stringValue(
              createIndexStatement(
                indexDescriptor.getName,
                indexType,
                entityType,
                indexInfo.labelsOrTypes,
                indexInfo.properties,
                providerName,
                indexDescriptor.getIndexConfig,
                indexInfo.indexStatus.maybeConstraint
              )
            )
          case unknown =>
            // This match should cover all existing columns but we get scala warnings
            // on non-exhaustive match due to it being string values
            throw new IllegalStateException(s"Missing case for column: $unknown")
        }.toMap[String, AnyValue]
    }
    val updatedRows = updateRowsWithPotentiallyRenamedColumns(rows.toList)
    ClosingIterator.apply(updatedRows.iterator)
  }

  private def getIndexStatistics(
    ctx: QueryContext,
    indexDescriptor: IndexDescriptor,
    zoneId: ZoneId
  ) = {
    def getAsTime(timeInMs: Long) =
      Values.temporalValue(formatTime(timeInMs, zoneId).toZonedDateTime)

    // If we need any of the statistics, fetch all statistics columns
    if (
      requestedColumnsNames.contains(lastReadColumn) ||
      requestedColumnsNames.contains(readCountColumn) ||
      requestedColumnsNames.contains(trackedSinceColumn)
    ) {
      val indexStatistics = ctx.getIndexUsageStatistics(indexDescriptor)
      val trackedSinceKernelValue = indexStatistics.trackedSince()
      val lastReadKernelValue = indexStatistics.lastRead()

      // Interpreting the kernel values into what SHOW INDEXES should return
      if (trackedSinceKernelValue == 0) {
        // not tracked at all: all columns null
        (Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE)
      } else if (lastReadKernelValue == 0) {
        // tracked but not yet read:
        // trackedSince should have real value, others default for when tracked but not seen
        (Values.NO_VALUE, Values.longValue(0L), getAsTime(trackedSinceKernelValue))
      } else {
        // all columns have available values
        (
          getAsTime(lastReadKernelValue),
          Values.longValue(indexStatistics.readCount()),
          getAsTime(trackedSinceKernelValue)
        )
      }
    } else (Values.NO_VALUE, Values.NO_VALUE, Values.NO_VALUE)
  }

}

object ShowIndexesCommand {

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

    indexType match {
      case IndexType.RANGE =>
        maybeConstraint match {
          case Some(constraint) if constraint.isNodeUniquenessConstraint =>
            createNodeConstraintCommand(name, labelsOrTypes, properties, "IS UNIQUE")
          case Some(constraint) if constraint.isRelationshipUniquenessConstraint =>
            createRelConstraintCommand(name, labelsOrTypes, properties, "IS UNIQUE")
          case Some(constraint) if constraint.isNodeKeyConstraint =>
            createNodeConstraintCommand(name, labelsOrTypes, properties, "IS NODE KEY")
          case Some(constraint) if constraint.isRelationshipKeyConstraint =>
            createRelConstraintCommand(name, labelsOrTypes, properties, "IS RELATIONSHIP KEY")
          case Some(_) =>
            throw new IllegalArgumentException(
              "Expected an index or index backed constraint, found another constraint."
            )
          case None =>
            entityType match {
              case EntityType.NODE =>
                createNodeIndexCommand("RANGE", name, labelsOrTypes, properties)
              case EntityType.RELATIONSHIP =>
                createRelIndexCommand("RANGE", name, labelsOrTypes, properties)
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
            createIndexCommand(
              "FULLTEXT",
              name,
              s"(n$labelsOrTypesWithBars)",
              s"EACH [$escapedNodeProperties]",
              Some(optionsString)
            )
          case EntityType.RELATIONSHIP =>
            val escapedRelProperties = asEscapedString(properties, relPropStringJoiner)
            createIndexCommand(
              "FULLTEXT",
              name,
              s"()-[r$labelsOrTypesWithBars]-()",
              s"EACH [$escapedRelProperties]",
              Some(optionsString)
            )
          case _ => throw new IllegalArgumentException(s"Did not recognize entity type $entityType")
        }
      case IndexType.TEXT =>
        val optionsString = s"{indexConfig: {}, indexProvider: '$providerName'}"

        entityType match {
          case EntityType.NODE =>
            createNodeIndexCommand("TEXT", name, labelsOrTypes, properties, Some(optionsString))
          case EntityType.RELATIONSHIP =>
            createRelIndexCommand("TEXT", name, labelsOrTypes, properties, Some(optionsString))
          case _ => throw new IllegalArgumentException(s"Did not recognize entity type $entityType")
        }
      case IndexType.POINT =>
        val pointConfig = configAsString(indexConfig, value => pointConfigValueAsString(value))
        val optionsString = optionsAsString(providerName, pointConfig)

        entityType match {
          case EntityType.NODE =>
            createNodeIndexCommand("POINT", name, labelsOrTypes, properties, Some(optionsString))
          case EntityType.RELATIONSHIP =>
            createRelIndexCommand("POINT", name, labelsOrTypes, properties, Some(optionsString))
          case _ => throw new IllegalArgumentException(s"Did not recognize entity type $entityType")
        }
      case IndexType.VECTOR =>
        val vectorConfig = configAsString(indexConfig, value => vectorConfigValueAsString(value))
        val optionsString = optionsAsString(providerName, vectorConfig)

        entityType match {
          case EntityType.NODE =>
            createNodeIndexCommand("VECTOR", name, labelsOrTypes, properties, Some(optionsString))
          case EntityType.RELATIONSHIP =>
            createRelIndexCommand("VECTOR", name, labelsOrTypes, properties, Some(optionsString))
          case _ => throw new IllegalArgumentException(s"Did not recognize entity type $entityType")
        }
      case IndexType.LOOKUP =>
        entityType match {
          case EntityType.NODE =>
            createIndexCommand("LOOKUP", name, "(n)", "EACH labels(n)")
          case EntityType.RELATIONSHIP =>
            createIndexCommand("LOOKUP", name, "()-[r]-()", "EACH type(r)")
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

  private def vectorConfigValueAsString(configValue: Value): String = {
    configValue match {
      case intValue: IntValue       => intValue.intValue().toString
      case longValue: LongValue     => longValue.longValue().toString
      case stringValue: StringValue => "'" + stringValue.stringValue() + "'"
      case _ => throw new IllegalArgumentException(s"Could not convert config value '$configValue' to config string.")
    }
  }
}
