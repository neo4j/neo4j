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
package org.neo4j.cypher.internal.compiler

import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.QualifiedName
import org.neo4j.cypher.internal.frontend.phases.UserFunctionSignature
import org.neo4j.cypher.internal.planner.spi.DatabaseMode.DatabaseMode
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.planner.spi.InstrumentedGraphStatistics
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.planner.spi.TokenIndexDescriptor
import org.neo4j.cypher.internal.util.InternalNotificationLogger
import org.neo4j.internal.schema.constraints.SchemaValueType

//noinspection NotImplementedCode
class NotImplementedPlanContext extends PlanContext {

  override def rangeIndexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] = ???

  override def rangeIndexesGetForRelType(relTypeId: Int): Iterator[IndexDescriptor] = ???

  override def textIndexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] = ???

  override def textIndexesGetForRelType(relTypeId: Int): Iterator[IndexDescriptor] = ???

  override def pointIndexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] = ???

  override def pointIndexesGetForRelType(relTypeId: Int): Iterator[IndexDescriptor] = ???

  override def propertyIndexesGetAll(): Iterator[IndexDescriptor] = ???

  override def indexExistsForLabel(labelId: Int): Boolean = ???

  override def indexExistsForRelType(labelId: Int): Boolean = ???

  override def nodeTokenIndex: Option[TokenIndexDescriptor] = ???

  override def relationshipTokenIndex: Option[TokenIndexDescriptor] = ???

  override def hasNodePropertyExistenceConstraint(labelName: String, propertyKey: String): Boolean = ???

  override def getNodePropertiesWithExistenceConstraint(labelName: String): Set[String] = ???

  override def hasRelationshipPropertyExistenceConstraint(labelName: String, propertyKey: String): Boolean = ???

  override def getRelationshipPropertiesWithExistenceConstraint(labelName: String): Set[String] = ???

  override def hasNodePropertyTypeConstraint(
    labelName: String,
    propertyKey: String,
    cypherType: SchemaValueType
  ): Boolean = ???

  override def getNodePropertiesWithTypeConstraint(labelName: String): Map[String, Seq[SchemaValueType]] = ???

  override def hasRelationshipPropertyTypeConstraint(
    relTypeName: String,
    propertyKey: String,
    cypherType: SchemaValueType
  ): Boolean = ???

  override def getRelationshipPropertiesWithTypeConstraint(relTypeName: String): Map[String, Seq[SchemaValueType]] = ???

  override def getPropertiesWithExistenceConstraint: Set[String] = ???

  override def lastCommittedTxIdProvider: () => Long = ???

  override def statistics: InstrumentedGraphStatistics = ???

  override def notificationLogger(): InternalNotificationLogger = ???

  override def txStateHasChanges(): Boolean = ???

  override def procedureSignature(name: QualifiedName): ProcedureSignature = ???

  override def functionSignature(name: QualifiedName): Option[UserFunctionSignature] = ???

  override def getLabelName(id: Int): String = ???

  override def getOptLabelId(labelName: String): Option[Int] = ???

  override def getLabelId(labelName: String): Int = ???

  override def getPropertyKeyName(id: Int): String = ???

  override def getOptPropertyKeyId(propertyKeyName: String): Option[Int] = ???

  override def getPropertyKeyId(propertyKeyName: String): Int = ???

  override def getRelTypeName(id: Int): String = ???

  override def getOptRelTypeId(relType: String): Option[Int] = ???

  override def getRelTypeId(relType: String): Int = ???

  override def textIndexGetForLabelAndProperties(
    labelName: String,
    propertyKeys: Seq[String]
  ): Option[IndexDescriptor] = ???

  override def textIndexGetForRelTypeAndProperties(
    relTypeName: String,
    propertyKeys: Seq[String]
  ): Option[IndexDescriptor] = ???

  override def textIndexExistsForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Boolean = ???

  override def textIndexExistsForRelTypeAndProperties(relTypeName: String, propertyKeys: Seq[String]): Boolean = ???

  override def rangeIndexExistsForRelTypeAndProperties(relTypeName: String, propertyKeys: Seq[String]): Boolean = ???

  override def rangeIndexGetForRelTypeAndProperties(
    relTypeName: String,
    propertyKeys: Seq[String]
  ): Option[IndexDescriptor] = ???

  override def rangeIndexExistsForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Boolean = ???

  override def rangeIndexGetForLabelAndProperties(
    labelName: String,
    propertyKeys: Seq[String]
  ): Option[IndexDescriptor] = ???

  override def pointIndexExistsForRelTypeAndProperties(relTypeName: String, propertyKeys: Seq[String]): Boolean = ???

  override def pointIndexGetForRelTypeAndProperties(
    relTypeName: String,
    propertyKeys: Seq[String]
  ): Option[IndexDescriptor] = ???

  override def pointIndexExistsForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Boolean = ???

  override def pointIndexGetForLabelAndProperties(
    labelName: String,
    propertyKeys: Seq[String]
  ): Option[IndexDescriptor] = ???

  override def procedureSignatureVersion: Long = ???

  override def withNotificationLogger(notificationLogger: InternalNotificationLogger): PlanContext = this

  override def databaseMode: DatabaseMode = ???
}
