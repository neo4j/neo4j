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
package org.neo4j.cypher.internal.spi

import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.QualifiedName
import org.neo4j.cypher.internal.frontend.phases.UserFunctionSignature
import org.neo4j.cypher.internal.macros.TranslateExceptionMacros.translateException
import org.neo4j.cypher.internal.planner.spi.DatabaseMode.DatabaseMode
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.planner.spi.InstrumentedGraphStatistics
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.planner.spi.TokenIndexDescriptor
import org.neo4j.cypher.internal.planning.ExceptionTranslationSupport
import org.neo4j.cypher.internal.util.InternalNotificationLogger
import org.neo4j.internal.schema.constraints.SchemaValueType

class ExceptionTranslatingPlanContext(inner: PlanContext) extends PlanContext with ExceptionTranslationSupport {

  override def rangeIndexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] =
    translateException(tokenNameLookup, inner.rangeIndexesGetForLabel(labelId))

  override def textIndexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] =
    translateException(tokenNameLookup, inner.textIndexesGetForLabel(labelId))

  override def pointIndexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] =
    translateException(tokenNameLookup, inner.pointIndexesGetForLabel(labelId))

  override def pointIndexesGetForRelType(relTypeId: Int): Iterator[IndexDescriptor] =
    translateException(tokenNameLookup, inner.pointIndexesGetForRelType(relTypeId))

  override def rangeIndexesGetForRelType(relTypeId: Int): Iterator[IndexDescriptor] =
    translateException(tokenNameLookup, inner.rangeIndexesGetForRelType(relTypeId))

  override def textIndexesGetForRelType(relTypeId: Int): Iterator[IndexDescriptor] =
    translateException(tokenNameLookup, inner.textIndexesGetForRelType(relTypeId))

  override def propertyIndexesGetAll(): Iterator[IndexDescriptor] =
    translateException(tokenNameLookup, inner.propertyIndexesGetAll())

  override def textIndexGetForLabelAndProperties(
    labelName: String,
    propertyKeys: Seq[String]
  ): Option[IndexDescriptor] =
    translateException(tokenNameLookup, inner.textIndexGetForLabelAndProperties(labelName, propertyKeys))

  override def rangeIndexGetForLabelAndProperties(
    labelName: String,
    propertyKeys: Seq[String]
  ): Option[IndexDescriptor] =
    translateException(tokenNameLookup, inner.rangeIndexGetForLabelAndProperties(labelName, propertyKeys))

  override def pointIndexGetForLabelAndProperties(
    labelName: String,
    propertyKeys: Seq[String]
  ): Option[IndexDescriptor] =
    translateException(tokenNameLookup, inner.pointIndexGetForLabelAndProperties(labelName, propertyKeys))

  override def textIndexGetForRelTypeAndProperties(
    relTypeName: String,
    propertyKeys: Seq[String]
  ): Option[IndexDescriptor] =
    translateException(tokenNameLookup, inner.textIndexGetForRelTypeAndProperties(relTypeName, propertyKeys))

  override def rangeIndexGetForRelTypeAndProperties(
    relTypeName: String,
    propertyKeys: Seq[String]
  ): Option[IndexDescriptor] =
    translateException(tokenNameLookup, inner.rangeIndexGetForRelTypeAndProperties(relTypeName, propertyKeys))

  override def pointIndexGetForRelTypeAndProperties(
    relTypeName: String,
    propertyKeys: Seq[String]
  ): Option[IndexDescriptor] =
    translateException(tokenNameLookup, inner.pointIndexGetForRelTypeAndProperties(relTypeName, propertyKeys))

  override def textIndexExistsForLabelAndProperties(labelName: String, propertyKey: Seq[String]): Boolean =
    translateException(tokenNameLookup, inner.textIndexExistsForLabelAndProperties(labelName, propertyKey))

  override def rangeIndexExistsForLabelAndProperties(labelName: String, propertyKey: Seq[String]): Boolean =
    translateException(tokenNameLookup, inner.rangeIndexExistsForLabelAndProperties(labelName, propertyKey))

  override def pointIndexExistsForLabelAndProperties(labelName: String, propertyKey: Seq[String]): Boolean =
    translateException(tokenNameLookup, inner.pointIndexExistsForLabelAndProperties(labelName, propertyKey))

  override def textIndexExistsForRelTypeAndProperties(relTypeName: String, propertyKey: Seq[String]): Boolean =
    translateException(tokenNameLookup, inner.textIndexExistsForRelTypeAndProperties(relTypeName, propertyKey))

  override def rangeIndexExistsForRelTypeAndProperties(relTypeName: String, propertyKeys: Seq[String]): Boolean =
    translateException(tokenNameLookup, inner.rangeIndexExistsForRelTypeAndProperties(relTypeName, propertyKeys))

  override def pointIndexExistsForRelTypeAndProperties(relTypeName: String, propertyKeys: Seq[String]): Boolean =
    translateException(tokenNameLookup, inner.pointIndexExistsForRelTypeAndProperties(relTypeName, propertyKeys))

  override def statistics: InstrumentedGraphStatistics =
    translateException(tokenNameLookup, inner.statistics)

  override def lastCommittedTxIdProvider: () => Long = {
    val innerTxProvider = translateException(tokenNameLookup, inner.lastCommittedTxIdProvider)
    () => translateException(tokenNameLookup, innerTxProvider())
  }

  override def procedureSignature(name: QualifiedName): ProcedureSignature =
    translateException(tokenNameLookup, inner.procedureSignature(name))

  override def functionSignature(name: QualifiedName): Option[UserFunctionSignature] =
    translateException(tokenNameLookup, inner.functionSignature(name))

  override def indexExistsForLabel(labelId: Int): Boolean =
    translateException(tokenNameLookup, inner.indexExistsForLabel(labelId))

  override def indexExistsForRelType(relTypeId: Int): Boolean =
    translateException(tokenNameLookup, inner.indexExistsForRelType(relTypeId))

  override def nodeTokenIndex: Option[TokenIndexDescriptor] =
    translateException(tokenNameLookup, inner.nodeTokenIndex)

  override def relationshipTokenIndex: Option[TokenIndexDescriptor] =
    translateException(tokenNameLookup, inner.relationshipTokenIndex)

  override def hasNodePropertyExistenceConstraint(labelName: String, propertyKey: String): Boolean =
    translateException(tokenNameLookup, inner.hasNodePropertyExistenceConstraint(labelName, propertyKey))

  override def getNodePropertiesWithExistenceConstraint(labelName: String): Set[String] =
    translateException(tokenNameLookup, inner.getNodePropertiesWithExistenceConstraint(labelName))

  override def hasRelationshipPropertyExistenceConstraint(labelName: String, propertyKey: String): Boolean =
    translateException(tokenNameLookup, inner.hasRelationshipPropertyExistenceConstraint(labelName, propertyKey))

  override def getRelationshipPropertiesWithExistenceConstraint(labelName: String): Set[String] =
    translateException(tokenNameLookup, inner.getRelationshipPropertiesWithExistenceConstraint(labelName))

  override def getPropertiesWithExistenceConstraint: Set[String] =
    translateException(tokenNameLookup, inner.getPropertiesWithExistenceConstraint)

  override def getOptRelTypeId(relType: String): Option[Int] =
    translateException(tokenNameLookup, inner.getOptRelTypeId(relType))

  override def getRelTypeName(id: Int): String =
    translateException(tokenNameLookup, inner.getRelTypeName(id))

  override def getRelTypeId(relType: String): Int =
    translateException(tokenNameLookup, inner.getRelTypeId(relType))

  override def getOptPropertyKeyId(propertyKeyName: String): Option[Int] =
    translateException(tokenNameLookup, inner.getOptPropertyKeyId(propertyKeyName))

  override def getLabelName(id: Int): String =
    translateException(tokenNameLookup, inner.getLabelName(id))

  override def getOptLabelId(labelName: String): Option[Int] =
    translateException(tokenNameLookup, inner.getOptLabelId(labelName))

  override def getPropertyKeyId(propertyKeyName: String): Int =
    translateException(tokenNameLookup, inner.getPropertyKeyId(propertyKeyName))

  override def getPropertyKeyName(id: Int): String =
    translateException(tokenNameLookup, inner.getPropertyKeyName(id))

  override def getLabelId(labelName: String): Int =
    translateException(tokenNameLookup, inner.getLabelId(labelName))

  override def notificationLogger(): InternalNotificationLogger =
    translateException(tokenNameLookup, inner.notificationLogger())

  override def txStateHasChanges(): Boolean =
    translateException(tokenNameLookup, inner.txStateHasChanges())

  override def hasNodePropertyTypeConstraint(
    labelName: String,
    propertyKey: String,
    cypherType: SchemaValueType
  ): Boolean =
    translateException(tokenNameLookup, inner.hasNodePropertyTypeConstraint(labelName, propertyKey, cypherType))

  override def getNodePropertiesWithTypeConstraint(labelName: String): Map[String, Seq[SchemaValueType]] =
    translateException(tokenNameLookup, inner.getNodePropertiesWithTypeConstraint(labelName))

  override def getRelationshipPropertiesWithTypeConstraint(relTypeName: String): Map[String, Seq[SchemaValueType]] =
    translateException(tokenNameLookup, inner.getRelationshipPropertiesWithTypeConstraint(relTypeName))

  override def hasRelationshipPropertyTypeConstraint(
    relTypeName: String,
    propertyKey: String,
    cypherType: SchemaValueType
  ): Boolean =
    translateException(
      tokenNameLookup,
      inner.hasRelationshipPropertyTypeConstraint(relTypeName, propertyKey, cypherType)
    )

  override def procedureSignatureVersion: Long = translateException(tokenNameLookup, inner.procedureSignatureVersion)

  override def withNotificationLogger(notificationLogger: InternalNotificationLogger): PlanContext =
    new ExceptionTranslatingPlanContext(
      inner.withNotificationLogger(notificationLogger)
    )

  override def databaseMode: DatabaseMode = translateException(
    tokenNameLookup,
    inner.databaseMode
  )
}
