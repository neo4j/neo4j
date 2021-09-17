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
package org.neo4j.cypher.internal.spi

import org.neo4j.cypher.internal.logical.plans.ProcedureSignature
import org.neo4j.cypher.internal.logical.plans.QualifiedName
import org.neo4j.cypher.internal.logical.plans.UserFunctionSignature
import org.neo4j.cypher.internal.macros.TranslateExceptionMacros.translateException
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.planner.spi.InstrumentedGraphStatistics
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.planning.ExceptionTranslationSupport
import org.neo4j.cypher.internal.util.InternalNotificationLogger

class ExceptionTranslatingPlanContext(inner: PlanContext) extends PlanContext with ExceptionTranslationSupport {

  override def btreeIndexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] =
    translateException(tokenNameLookup, inner.btreeIndexesGetForLabel(labelId))

  override def btreeIndexesGetForRelType(relTypeId: Int): Iterator[IndexDescriptor] =
    translateException(tokenNameLookup, inner.btreeIndexesGetForRelType(relTypeId))

  override def textIndexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] =
    translateException(tokenNameLookup, inner.textIndexesGetForLabel(labelId))

  override def propertyIndexesGetAll(): Iterator[IndexDescriptor] =
    translateException(tokenNameLookup, inner.propertyIndexesGetAll())

  override def textIndexesGetForRelType(relTypeId: Int): Iterator[IndexDescriptor] =
    translateException(tokenNameLookup, inner.textIndexesGetForRelType(relTypeId))

  override def btreeIndexGetForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Option[IndexDescriptor] =
    translateException(tokenNameLookup, inner.btreeIndexGetForLabelAndProperties(labelName, propertyKeys))

  override def btreeIndexGetForRelTypeAndProperties(relTypeName: String, propertyKeys: Seq[String]): Option[IndexDescriptor] =
    translateException(tokenNameLookup, inner.btreeIndexGetForRelTypeAndProperties(relTypeName, propertyKeys))

  override def btreeIndexExistsForLabelAndProperties(labelName: String, propertyKey: Seq[String]): Boolean =
    translateException(tokenNameLookup, inner.btreeIndexExistsForLabelAndProperties(labelName, propertyKey))

  override def btreeIndexExistsForRelTypeAndProperties(relTypeName: String, propertyKey: Seq[String]): Boolean =
    translateException(tokenNameLookup, inner.btreeIndexExistsForRelTypeAndProperties(relTypeName, propertyKey))

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

  override def btreeIndexExistsForLabel(labelId: Int): Boolean =
    translateException(tokenNameLookup, inner.btreeIndexExistsForLabel(labelId))

  override def btreeIndexExistsForRelType(relTypeId: Int): Boolean =
    translateException(tokenNameLookup, inner.btreeIndexExistsForRelType(relTypeId))

  override def canLookupNodesByLabel: Boolean =
    translateException(tokenNameLookup, inner.canLookupNodesByLabel)

  override def canLookupRelationshipsByType: Boolean =
    translateException(tokenNameLookup, inner.canLookupRelationshipsByType)

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
}
