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
import org.neo4j.cypher.internal.ast.AllConstraints
import org.neo4j.cypher.internal.ast.CommandResultItem
import org.neo4j.cypher.internal.ast.ExistsConstraints
import org.neo4j.cypher.internal.ast.KeyConstraints
import org.neo4j.cypher.internal.ast.NodeExistsConstraints
import org.neo4j.cypher.internal.ast.NodeKeyConstraints
import org.neo4j.cypher.internal.ast.NodePropTypeConstraints
import org.neo4j.cypher.internal.ast.NodeUniqueConstraints
import org.neo4j.cypher.internal.ast.PropTypeConstraints
import org.neo4j.cypher.internal.ast.RelExistsConstraints
import org.neo4j.cypher.internal.ast.RelKeyConstraints
import org.neo4j.cypher.internal.ast.RelPropTypeConstraints
import org.neo4j.cypher.internal.ast.RelUniqueConstraints
import org.neo4j.cypher.internal.ast.ShowColumn
import org.neo4j.cypher.internal.ast.ShowConstraintType
import org.neo4j.cypher.internal.ast.ShowConstraintsClause.createStatementColumn
import org.neo4j.cypher.internal.ast.ShowConstraintsClause.entityTypeColumn
import org.neo4j.cypher.internal.ast.ShowConstraintsClause.idColumn
import org.neo4j.cypher.internal.ast.ShowConstraintsClause.labelsOrTypesColumn
import org.neo4j.cypher.internal.ast.ShowConstraintsClause.nameColumn
import org.neo4j.cypher.internal.ast.ShowConstraintsClause.optionsColumn
import org.neo4j.cypher.internal.ast.ShowConstraintsClause.ownedIndexColumn
import org.neo4j.cypher.internal.ast.ShowConstraintsClause.propertiesColumn
import org.neo4j.cypher.internal.ast.ShowConstraintsClause.propertyTypeColumn
import org.neo4j.cypher.internal.ast.ShowConstraintsClause.typeColumn
import org.neo4j.cypher.internal.ast.UniqueConstraints
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.ConstraintInfo
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowConstraintsCommand.createConstraintStatement
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowConstraintsCommand.getConstraintType
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.createNodeConstraintCommand
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.createRelConstraintCommand
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.extractOptionsMap
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.internal.schema
import org.neo4j.internal.schema.ConstraintDescriptor
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues

import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters.SeqHasAsJava

// SHOW [
//   ALL
//   | NODE UNIQUE | RELATIONSHIP UNIQUE | UNIQUE
//   | NODE EXIST | RELATIONSHIP EXIST | EXIST
//   | NODE KEY | RELATIONSHIP KEY | KEY
//   | NODE PROPERTY TYPE | RELATIONSHIP PROPERTY TYPE | PROPERTY TYPE
// ] CONSTRAINT[S] [BRIEF | VERBOSE | WHERE clause | YIELD clause]
case class ShowConstraintsCommand(
  constraintType: ShowConstraintType,
  columns: List[ShowColumn],
  yieldColumns: List[CommandResultItem]
) extends Command(columns, yieldColumns) {

  override def originalNameRows(state: QueryState, baseRow: CypherRow): ClosingIterator[Map[String, AnyValue]] = {
    val ctx = state.query
    ctx.assertShowConstraintAllowed()
    val constraints = ctx.getAllConstraints()
    val indexIdToName = ctx.getAllIndexes()
      .map { case (descriptor, _) => descriptor.getId -> descriptor.getName }

    val predicate: ConstraintDescriptor => Boolean = constraintType match {
      case UniqueConstraints => c => c.`type`().equals(schema.ConstraintType.UNIQUE)
      case NodeUniqueConstraints =>
        c => c.`type`().equals(schema.ConstraintType.UNIQUE) && c.schema.entityType.equals(EntityType.NODE)
      case RelUniqueConstraints =>
        c => c.`type`().equals(schema.ConstraintType.UNIQUE) && c.schema.entityType.equals(EntityType.RELATIONSHIP)
      case KeyConstraints => c => c.`type`().equals(schema.ConstraintType.UNIQUE_EXISTS)
      case NodeKeyConstraints =>
        c => c.`type`().equals(schema.ConstraintType.UNIQUE_EXISTS) && c.schema.entityType.equals(EntityType.NODE)
      case RelKeyConstraints => c =>
          c.`type`().equals(schema.ConstraintType.UNIQUE_EXISTS) && c.schema.entityType.equals(EntityType.RELATIONSHIP)
      case ExistsConstraints => c => c.`type`().equals(schema.ConstraintType.EXISTS)
      case NodeExistsConstraints =>
        c => c.`type`().equals(schema.ConstraintType.EXISTS) && c.schema.entityType.equals(EntityType.NODE)
      case RelExistsConstraints =>
        c => c.`type`().equals(schema.ConstraintType.EXISTS) && c.schema.entityType.equals(EntityType.RELATIONSHIP)
      case PropTypeConstraints => c => c.`type`().equals(schema.ConstraintType.PROPERTY_TYPE)
      case NodePropTypeConstraints =>
        c => c.`type`().equals(schema.ConstraintType.PROPERTY_TYPE) && c.schema.entityType.equals(EntityType.NODE)
      case RelPropTypeConstraints =>
        c =>
          c.`type`().equals(schema.ConstraintType.PROPERTY_TYPE) && c.schema.entityType.equals(EntityType.RELATIONSHIP)
      case AllConstraints => _ => true // Should keep all and not filter away any constraints
      case c              => throw new IllegalStateException(s"Unknown constraint type: $c")
    }

    val relevantConstraints = constraints.filter {
      case (constraintDescriptor, _) => predicate(constraintDescriptor)
    }
    val sortedRelevantConstraints: ListMap[ConstraintDescriptor, ConstraintInfo] =
      ListMap(relevantConstraints.toSeq.sortBy(_._1.getName): _*)

    val rows = sortedRelevantConstraints.map {
      case (constraintDescriptor: ConstraintDescriptor, constraintInfo: ConstraintInfo) =>
        val propertyType =
          if (
            constraintDescriptor.isPropertyTypeConstraint &&
            // only fetch value if we need it
            (requestedColumnsNames.contains(propertyTypeColumn) ||
              requestedColumnsNames.contains(createStatementColumn))
          )
            Some(
              constraintDescriptor.asPropertyTypeConstraint().propertyType().userDescription()
            )
          else None
        // These don't really have a default/fallback and is used in multiple columns
        // so let's keep them as is regardless of if they are actually needed or not
        val entityType = constraintDescriptor.schema.entityType
        val constraintType = getConstraintType(constraintDescriptor.`type`, entityType)

        requestedColumnsNames.map {
          // The id of the constraint, or null if created in transaction
          case `idColumn` =>
            val id =
              if (constraintIsAddedInTransaction(ctx, constraintDescriptor)) Values.NO_VALUE
              else Values.longValue(constraintDescriptor.getId)
            idColumn -> id
          // Name of the constraint, for example "myConstraint"
          case `nameColumn` => nameColumn -> Values.stringValue(constraintDescriptor.getName)
          // The ConstraintType of this constraint, one of "UNIQUENESS", "RELATIONSHIP_UNIQUENESS", "NODE_KEY", "RELATIONSHIP_KEY", "NODE_PROPERTY_EXISTENCE", "RELATIONSHIP_PROPERTY_EXISTENCE"
          case `typeColumn` => typeColumn -> Values.stringValue(constraintType.output)
          // Type of entities this constraint represents, either "NODE" or "RELATIONSHIP"
          case `entityTypeColumn` => entityTypeColumn -> Values.stringValue(entityType.name)
          // The labels or relationship types of this constraint, for example ["Label1", "Label2"] or ["RelType1", "RelType2"]
          case `labelsOrTypesColumn` => labelsOrTypesColumn -> VirtualValues.fromList(
              constraintInfo.labelsOrTypes.map(elem => Values.of(elem).asInstanceOf[AnyValue]).asJava
            )
          // The properties of this constraint, for example ["propKey", "propKey2"]
          case `propertiesColumn` => propertiesColumn -> VirtualValues.fromList(
              constraintInfo.properties.map(prop => Values.of(prop).asInstanceOf[AnyValue]).asJava
            )
          // The name of the index associated to the constraint
          case `ownedIndexColumn` =>
            val ownedIndex =
              if (constraintDescriptor.isIndexBackedConstraint)
                indexIdToName.get(constraintDescriptor.asIndexBackedConstraint().ownedIndexId())
                  .map(Values.stringValue)
                  .getOrElse(Values.NO_VALUE)
              else Values.NO_VALUE
            ownedIndexColumn -> ownedIndex
          // The Cypher type this constraint restricts its property to
          case `propertyTypeColumn` =>
            propertyTypeColumn -> propertyType.map(Values.stringValue).getOrElse(Values.NO_VALUE)
          // The options for this constraint, shows index provider and config of the backing index
          case `optionsColumn` => optionsColumn -> getOptions(constraintDescriptor, constraintInfo)
          // The statement to recreate the constraint
          case `createStatementColumn` =>
            val createString = createConstraintStatement(
              constraintDescriptor.getName,
              constraintType,
              constraintInfo.labelsOrTypes,
              constraintInfo.properties,
              propertyType
            )
            createStatementColumn -> Values.stringValue(createString)
          case unknown =>
            // This match should cover all existing columns but we get scala warnings
            // on non-exhaustive match due to it being string values
            throw new IllegalStateException(s"Missing case for column: $unknown")
        }.toMap[String, AnyValue]
    }
    val updatedRows = updateRowsWithPotentiallyRenamedColumns(rows.toList)
    ClosingIterator.apply(updatedRows.iterator)
  }

  private def getOptions(
    constraintDescriptor: ConstraintDescriptor,
    constraintInfo: ConstraintInfo
  ) = {
    if (constraintDescriptor.isIndexBackedConstraint) {
      val index = constraintInfo.maybeIndex.getOrElse(
        throw new IllegalStateException(
          s"Expected to find an index for index backed constraint ${constraintDescriptor.getName}"
        )
      )
      val providerName = index.getIndexProvider.name
      val indexConfig = index.getIndexConfig
      extractOptionsMap(providerName, indexConfig)
    } else Values.NO_VALUE
  }

  private def constraintIsAddedInTransaction(ctx: QueryContext, constraintDescriptor: ConstraintDescriptor): Boolean =
    Option(ctx.transactionalContext.kernelQueryContext.getTransactionStateOrNull)
      .map(_.constraintsChanges)
      .exists(_.isAdded(constraintDescriptor))
}

object ShowConstraintsCommand {

  private def createConstraintStatement(
    name: String,
    constraintType: ShowConstraintType,
    labelsOrTypes: List[String],
    properties: List[String],
    propertyType: Option[String]
  ): String = {
    constraintType match {
      case NodeUniqueConstraints =>
        createNodeConstraintCommand(name, labelsOrTypes, properties, "IS UNIQUE")
      case RelUniqueConstraints =>
        createRelConstraintCommand(name, labelsOrTypes, properties, "IS UNIQUE")
      case NodeKeyConstraints =>
        createNodeConstraintCommand(name, labelsOrTypes, properties, "IS NODE KEY")
      case RelKeyConstraints =>
        createRelConstraintCommand(name, labelsOrTypes, properties, "IS RELATIONSHIP KEY")
      case NodeExistsConstraints =>
        createNodeConstraintCommand(name, labelsOrTypes, properties, "IS NOT NULL")
      case RelExistsConstraints =>
        createRelConstraintCommand(name, labelsOrTypes, properties, "IS NOT NULL")
      case NodePropTypeConstraints =>
        val typeString = propertyType.getOrElse(
          throw new IllegalArgumentException(s"Expected a property type for $constraintType constraint.")
        )
        createNodeConstraintCommand(name, labelsOrTypes, properties, s"IS :: $typeString")
      case RelPropTypeConstraints =>
        val typeString = propertyType.getOrElse(
          throw new IllegalArgumentException(s"Expected a property type for $constraintType constraint.")
        )
        createRelConstraintCommand(name, labelsOrTypes, properties, s"IS :: $typeString")
      case _ => throw new IllegalArgumentException(
          s"Did not expect constraint type ${constraintType.prettyPrint} for constraint create command."
        )
    }
  }

  private def getConstraintType(
    internalConstraintType: schema.ConstraintType,
    entityType: EntityType
  ): ShowConstraintType = {
    (internalConstraintType, entityType) match {
      case (schema.ConstraintType.UNIQUE, EntityType.NODE)                => NodeUniqueConstraints
      case (schema.ConstraintType.UNIQUE, EntityType.RELATIONSHIP)        => RelUniqueConstraints
      case (schema.ConstraintType.UNIQUE_EXISTS, EntityType.NODE)         => NodeKeyConstraints
      case (schema.ConstraintType.UNIQUE_EXISTS, EntityType.RELATIONSHIP) => RelKeyConstraints
      case (schema.ConstraintType.EXISTS, EntityType.NODE)                => NodeExistsConstraints
      case (schema.ConstraintType.EXISTS, EntityType.RELATIONSHIP)        => RelExistsConstraints
      case (schema.ConstraintType.PROPERTY_TYPE, EntityType.NODE)         => NodePropTypeConstraints
      case (schema.ConstraintType.PROPERTY_TYPE, EntityType.RELATIONSHIP) => RelPropTypeConstraints
      case _ => throw new IllegalStateException(
          s"Invalid constraint combination: ConstraintType $internalConstraintType and EntityType $entityType."
        )
    }
  }
}
