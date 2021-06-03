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
import org.neo4j.cypher.internal.ast.AllConstraints
import org.neo4j.cypher.internal.ast.ExistsConstraints
import org.neo4j.cypher.internal.ast.NodeExistsConstraints
import org.neo4j.cypher.internal.ast.NodeKeyConstraints
import org.neo4j.cypher.internal.ast.RelExistsConstraints
import org.neo4j.cypher.internal.ast.ShowColumn
import org.neo4j.cypher.internal.ast.ShowConstraintType
import org.neo4j.cypher.internal.ast.UniqueConstraints
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.ConstraintInfo
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowConstraintsCommand.createConstraintStatement
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowConstraintsCommand.getConstraintType
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.asEscapedString
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.btreeConfigValueAsString
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.colonStringJoiner
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.configAsString
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.escapeBackticks
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.extractOptionsMap
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.optionsAsString
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.propStringJoiner
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.relPropStringJoiner
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.internal.schema
import org.neo4j.internal.schema.ConstraintDescriptor
import org.neo4j.internal.schema.IndexConfig
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues

import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.collection.immutable.ListMap

// SHOW [ALL|UNIQUE|NODE EXIST|RELATIONSHIP EXIST|EXIST|NODE KEY] CONSTRAINT[S] [BRIEF|VERBOSE|WHERE clause|YIELD clause]
case class ShowConstraintsCommand(constraintType: ShowConstraintType, verbose: Boolean, columns: List[ShowColumn]) extends Command(columns) {
  override def originalNameRows(state: QueryState): ClosingIterator[Map[String, AnyValue]] = {
    val ctx = state.query
    ctx.assertShowConstraintAllowed()
    val constraints = ctx.getAllConstraints()

    val predicate: ConstraintDescriptor => Boolean = constraintType match {
      case UniqueConstraints => c => c.`type`().equals(schema.ConstraintType.UNIQUE)
      case NodeKeyConstraints => c => c.`type`().equals(schema.ConstraintType.UNIQUE_EXISTS)
      case _: ExistsConstraints => c => c.`type`().equals(schema.ConstraintType.EXISTS)
      case _: NodeExistsConstraints => c => c.`type`().equals(schema.ConstraintType.EXISTS) && c.schema.entityType.equals(EntityType.NODE)
      case _: RelExistsConstraints => c => c.`type`().equals(schema.ConstraintType.EXISTS) && c.schema.entityType.equals(EntityType.RELATIONSHIP)
      case AllConstraints => _ => true // Should keep all and not filter away any constraints
      case c => throw new IllegalStateException(s"Unknown constraint type: $c")
    }

    val relevantConstraints = constraints.filter {
      case (constraintDescriptor, _) => predicate(constraintDescriptor)
    }
    val sortedRelevantConstraints: ListMap[ConstraintDescriptor, ConstraintInfo] = ListMap(relevantConstraints.toSeq.sortBy(_._1.getName):_*)

    val rows = sortedRelevantConstraints.map {
      case (constraintDescriptor: ConstraintDescriptor, constraintInfo: ConstraintInfo) =>
        val name = constraintDescriptor.getName
        val labels = constraintInfo.labelsOrTypes
        val properties = constraintInfo.properties
        val isIndexBacked = constraintDescriptor.isIndexBackedConstraint
        val entityType = constraintDescriptor.schema.entityType
        val constraintType = getConstraintType(constraintDescriptor.`type`, entityType)

        val briefResult = Map(
          // The id of the constraint
          "id" -> Values.longValue(constraintDescriptor.getId),
          // Name of the constraint, for example "myConstraint"
          "name" -> Values.stringValue(name),
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
              throw new IllegalStateException(s"Expected to find an index for index backed constraint $name")
            )
            val providerName = index.getIndexProvider.name
            val indexConfig = index.getIndexConfig
            val options: MapValue = extractOptionsMap(providerName, indexConfig)
            val createWithOptions = createConstraintStatement(name, constraintType, labels, properties, Some(providerName), Some(indexConfig))
            (options, createWithOptions)
          } else {
            val createWithoutOptions = createConstraintStatement(name, constraintType, labels, properties)
            (Values.NO_VALUE, createWithoutOptions)
          }

          briefResult ++ Map(
            "options" -> options,
            "createStatement" -> Values.stringValue(createString)
          )
        } else {
          briefResult
        }
    }
    ClosingIterator.apply(rows.iterator)
  }
}

object ShowConstraintsCommand {
  private def createConstraintStatement(name: String,
                                        constraintType: ShowConstraintType,
                                        labelsOrTypes: List[String],
                                        properties: List[String],
                                        providerName: Option[String] = None,
                                        indexConfig: Option[IndexConfig] = None): String = {
    val labelsOrTypesWithColons = asEscapedString(labelsOrTypes, colonStringJoiner)
    val escapedName = escapeBackticks(name)
    constraintType match {
      case UniqueConstraints =>
        val escapedProperties = asEscapedString(properties, propStringJoiner)
        val options = extractOptionsString(providerName, indexConfig, UniqueConstraints.prettyPrint)
        s"CREATE CONSTRAINT `$escapedName` ON (n$labelsOrTypesWithColons) ASSERT ($escapedProperties) IS UNIQUE OPTIONS $options"
      case NodeKeyConstraints =>
        val escapedProperties = asEscapedString(properties, propStringJoiner)
        val options = extractOptionsString(providerName, indexConfig, NodeKeyConstraints.prettyPrint)
        s"CREATE CONSTRAINT `$escapedName` ON (n$labelsOrTypesWithColons) ASSERT ($escapedProperties) IS NODE KEY OPTIONS $options"
      case _: NodeExistsConstraints =>
        val escapedProperties = asEscapedString(properties, propStringJoiner)
        s"CREATE CONSTRAINT `$escapedName` ON (n$labelsOrTypesWithColons) ASSERT ($escapedProperties) IS NOT NULL"
      case _: RelExistsConstraints =>
        val escapedProperties = asEscapedString(properties, relPropStringJoiner)
        s"CREATE CONSTRAINT `$escapedName` ON ()-[r$labelsOrTypesWithColons]-() ASSERT ($escapedProperties) IS NOT NULL"
      case _ => throw new IllegalArgumentException(s"Did not expect constraint type ${constraintType.prettyPrint} for constraint create command.")
    }
  }

  private def extractOptionsString(maybeProviderName: Option[String], maybeIndexConfig: Option[IndexConfig], constraintType: String): String = {
    val providerName = maybeProviderName.getOrElse(throw new IllegalArgumentException(s"Expected a provider name for $constraintType constraint."))
    val indexConfig = maybeIndexConfig.getOrElse(throw new IllegalArgumentException(s"Expected an index configuration for $constraintType constraint."))
    val btreeConfig = configAsString(indexConfig, value => btreeConfigValueAsString(value))
    optionsAsString(providerName, btreeConfig)
  }

  private def getConstraintType(internalConstraintType: schema.ConstraintType, entityType: EntityType): ShowConstraintType = {
    (internalConstraintType, entityType) match {
      case (schema.ConstraintType.UNIQUE, EntityType.NODE) => UniqueConstraints
      case (schema.ConstraintType.UNIQUE_EXISTS, EntityType.NODE) => NodeKeyConstraints
      case (schema.ConstraintType.EXISTS, EntityType.NODE) => NodeExistsConstraints()
      case (schema.ConstraintType.EXISTS, EntityType.RELATIONSHIP) => RelExistsConstraints()
      case _ => throw new IllegalStateException(s"Invalid constraint combination: ConstraintType $internalConstraintType and EntityType $entityType.")
    }
  }
}
