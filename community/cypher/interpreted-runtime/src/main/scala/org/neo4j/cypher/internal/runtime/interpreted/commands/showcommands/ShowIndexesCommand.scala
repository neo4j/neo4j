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
package org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands

import org.neo4j.common.EntityType
import org.neo4j.cypher.internal.ast.AllIndexes
import org.neo4j.cypher.internal.ast.BtreeIndexes
import org.neo4j.cypher.internal.ast.FulltextIndexes
import org.neo4j.cypher.internal.ast.LookupIndexes
import org.neo4j.cypher.internal.ast.ShowColumn
import org.neo4j.cypher.internal.ast.ShowIndexType
import org.neo4j.cypher.internal.ast.TextIndexes
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.IndexInfo
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowIndexesCommand.Nonunique
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowIndexesCommand.Unique
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowIndexesCommand.createIndexStatement
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.asEscapedString
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.barStringJoiner
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.btreeConfigValueAsString
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.colonStringJoiner
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.configAsString
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.escapeBackticks
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.extractOptionsMap
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.optionsAsString
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.propStringJoiner
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.relPropStringJoiner
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.internal.schema.ConstraintDescriptor
import org.neo4j.internal.schema.IndexConfig
import org.neo4j.internal.schema.IndexDescriptor
import org.neo4j.internal.schema.IndexType
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.StringValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues

import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.collection.immutable.ListMap

// SHOW [ALL|BTREE|FULLTEXT|LOOKUP] INDEX[ES] [BRIEF|VERBOSE|WHERE clause|YIELD clause]
case class ShowIndexesCommand(indexType: ShowIndexType, verbose: Boolean, columns: List[ShowColumn]) extends Command(columns) {
  override def originalNameRows(state: QueryState): ClosingIterator[Map[String, AnyValue]] = {
    val ctx = state.query
    ctx.assertShowIndexAllowed()
    val indexes: Map[IndexDescriptor, IndexInfo] = ctx.getAllIndexes()
    val relevantIndexes = indexType match {
      case AllIndexes => indexes
      case BtreeIndexes =>
        indexes.filter {
          case (indexDescriptor, _) => indexDescriptor.getIndexType.equals(IndexType.BTREE)
        }
      case FulltextIndexes =>
        indexes.filter {
          case (indexDescriptor, _) => indexDescriptor.getIndexType.equals(IndexType.FULLTEXT)
        }
      case TextIndexes =>
        indexes.filter {
          case (indexDescriptor, _) => indexDescriptor.getIndexType.equals(IndexType.TEXT)
        }
      case LookupIndexes =>
        indexes.filter {
          case (indexDescriptor, _) => indexDescriptor.getIndexType.equals(IndexType.LOOKUP)
        }
    }

    val sortedRelevantIndexes: ListMap[IndexDescriptor, IndexInfo] = ListMap(relevantIndexes.toSeq.sortBy(_._1.getName): _*)
    val rows = sortedRelevantIndexes.map {
      case (indexDescriptor: IndexDescriptor, indexInfo: IndexInfo) =>
        val indexStatus = indexInfo.indexStatus
        val uniqueness = if (indexDescriptor.isUnique) Unique.toString else Nonunique.toString
        val indexType = indexDescriptor.getIndexType
        val isLookupIndex = indexType.equals(IndexType.LOOKUP)

        val name = indexDescriptor.getName

        val entityType = indexDescriptor.schema.entityType
        val labelsOrTypes = indexInfo.labelsOrTypes
        val properties = indexInfo.properties
        val providerName = indexDescriptor.getIndexProvider.name
        val labelsOrTypesValue = if (isLookupIndex) Values.NO_VALUE
                                 else VirtualValues.fromList(labelsOrTypes.map(elem => Values.of(elem).asInstanceOf[AnyValue]).asJava)
        val propertiesValue = if (isLookupIndex) Values.NO_VALUE
                              else VirtualValues.fromList(properties.map(prop => Values.of(prop).asInstanceOf[AnyValue]).asJava)

        val briefResult = Map(
          // The id of the index
          "id" -> Values.longValue(indexDescriptor.getId),
          // Name of the index, for example "myIndex"
          "name" -> Values.stringValue(name),
          // Current state of the index, one of "ONLINE", "FAILED", "POPULATING"
          "state" -> Values.stringValue(indexStatus.state),
          // % of index population, for example 0.0, 100.0, or 75.1
          "populationPercent" -> Values.doubleValue(indexStatus.populationProgress),
          // Tells if the index is only meant to allow one value per key, either "UNIQUE" or "NONUNIQUE"
          "uniqueness" -> Values.stringValue(uniqueness),
          // The IndexType of this index, either "FULLTEXT", "TEXT", "BTREE" or "LOOKUP"
          "type" -> Values.stringValue(indexType.name),
          // Type of entities this index represents, either "NODE" or "RELATIONSHIP"
          "entityType" -> Values.stringValue(entityType.name),
          // The labels or relationship types of this constraint, for example ["Label1", "Label2"] or ["RelType1", "RelType2"], null for lookup indexes
          "labelsOrTypes" -> labelsOrTypesValue,
          // The properties of this constraint, for example ["propKey", "propKey2"], null for lookup indexes
          "properties" -> propertiesValue,
          // The index provider for this index, one of "native-btree-1.0", "lucene+native-3.0", "fulltext-1.0", "token-lookup-1.0"
          "indexProvider" -> Values.stringValue(providerName)
        )
        if (verbose) {
          val indexConfig = indexDescriptor.getIndexConfig
          val optionsValue = extractOptionsMap(providerName, indexConfig)
          briefResult ++ Map(
            "options" -> optionsValue,
            "failureMessage" -> Values.stringValue(indexStatus.failureMessage),
            "createStatement" -> Values.stringValue(
              createIndexStatement(name, indexType, entityType, labelsOrTypes, properties, providerName, indexConfig, indexStatus.maybeConstraint))
          )
        } else {
          briefResult
        }
    }
    ClosingIterator.apply(rows.iterator)
  }

}

object ShowIndexesCommand {
  sealed trait Uniqueness

  case object Unique extends Uniqueness {
    override final val toString: String = "UNIQUE"
  }

  case object Nonunique extends Uniqueness {
    override final val toString: String = "NONUNIQUE"
  }

  private def createIndexStatement(name: String,
                                   indexType: IndexType,
                                   entityType: EntityType,
                                   labelsOrTypes: List[String],
                                   properties: List[String],
                                   providerName: String,
                                   indexConfig: IndexConfig,
                                   maybeConstraint: Option[ConstraintDescriptor]): String = {

    val escapedName = s"`${escapeBackticks(name)}`"
    indexType match {
      case IndexType.BTREE =>
        val labelsOrTypesWithColons = asEscapedString(labelsOrTypes, colonStringJoiner)
        val escapedNodeProperties = asEscapedString(properties, propStringJoiner)

        val btreeConfig = configAsString(indexConfig, value => btreeConfigValueAsString(value))
        val optionsString = optionsAsString(providerName, btreeConfig)

        maybeConstraint match {
          case Some(constraint) if constraint.isUniquenessConstraint =>
            s"CREATE CONSTRAINT $escapedName FOR (n$labelsOrTypesWithColons) REQUIRE ($escapedNodeProperties) IS UNIQUE OPTIONS $optionsString"
          case Some(constraint) if constraint.isNodeKeyConstraint =>
            s"CREATE CONSTRAINT $escapedName FOR (n$labelsOrTypesWithColons) REQUIRE ($escapedNodeProperties) IS NODE KEY OPTIONS $optionsString"
          case Some(_) =>
            throw new IllegalArgumentException("Expected an index or index backed constraint, found another constraint.")
          case None =>
            entityType match {
              case EntityType.NODE =>
                s"CREATE INDEX $escapedName FOR (n$labelsOrTypesWithColons) ON ($escapedNodeProperties) OPTIONS $optionsString"
              case EntityType.RELATIONSHIP =>
                val escapedRelProperties = asEscapedString(properties, relPropStringJoiner)
                s"CREATE INDEX $escapedName FOR ()-[r$labelsOrTypesWithColons]-() ON ($escapedRelProperties) OPTIONS $optionsString"
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

        entityType match {
          case EntityType.NODE =>
            val escapedNodeProperties = asEscapedString(properties, propStringJoiner)
            s"CREATE TEXT INDEX $escapedName FOR (n$labelsOrTypesWithColons) ON ($escapedNodeProperties)"
          case EntityType.RELATIONSHIP =>
            val escapedRelProperties = asEscapedString(properties, relPropStringJoiner)
            s"CREATE TEXT INDEX $escapedName FOR ()-[r$labelsOrTypesWithColons]-() ON ($escapedRelProperties)"
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
      case stringValue: StringValue =>"'" + stringValue.stringValue() + "'"
      case _ => throw new IllegalArgumentException(s"Could not convert config value '$configValue' to config string.")
    }
  }
}
