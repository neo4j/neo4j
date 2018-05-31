/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.compatibility.v3_5

import org.opencypher.v9_0.frontend.phases.InternalNotificationLogger
import org.neo4j.cypher.internal.planner.v3_5.spi.{GraphStatistics, IndexDescriptor, PlanContext}
import org.neo4j.cypher.internal.v3_5.logical.plans.{ProcedureSignature, QualifiedName, UserFunctionSignature}

class ExceptionTranslatingPlanContext(inner: PlanContext) extends PlanContext with ExceptionTranslationSupport {

  override def indexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] =
    translateException(inner.indexesGetForLabel(labelId))

  override def indexExistsForLabelAndProperties(labelName: String, propertyKey: Seq[String]): Boolean =
    translateException(inner.indexExistsForLabelAndProperties(labelName, propertyKey))

  override def uniqueIndexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] =
    translateException(inner.uniqueIndexesGetForLabel(labelId))

  override def statistics: GraphStatistics =
    translateException(inner.statistics)

  override def checkNodeIndex(idxName: String): Unit =
    translateException(inner.checkNodeIndex(idxName))

  override def txIdProvider: () => Long = {
    val innerTxProvider = translateException(inner.txIdProvider)
    () => translateException(innerTxProvider())
  }

  override def procedureSignature(name: QualifiedName): ProcedureSignature =
    translateException(inner.procedureSignature(name))

  override def functionSignature(name: QualifiedName): Option[UserFunctionSignature] =
    translateException(inner.functionSignature(name))

  override def indexExistsForLabel(labelId: Int): Boolean =
    translateException(inner.indexExistsForLabel(labelId))

  override def hasPropertyExistenceConstraint(labelName: String, propertyKey: String): Boolean =
    translateException(inner.hasPropertyExistenceConstraint(labelName, propertyKey))

  override def checkRelIndex(idxName: String): Unit =
    translateException(inner.checkRelIndex(idxName))

  override def getOrCreateFromSchemaState[T](key: Any, f: => T): T =
    translateException(inner.getOrCreateFromSchemaState(key, f))

  override def getOptRelTypeId(relType: String): Option[Int] =
    translateException(inner.getOptRelTypeId(relType))

  override def getRelTypeName(id: Int): String =
    translateException(inner.getRelTypeName(id))

  override def getRelTypeId(relType: String): Int =
    translateException(inner.getRelTypeId(relType))

  override def getOptPropertyKeyId(propertyKeyName: String): Option[Int] =
    translateException(inner.getOptPropertyKeyId(propertyKeyName))

  override def getLabelName(id: Int): String =
    translateException(inner.getLabelName(id))

  override def getOptLabelId(labelName: String): Option[Int] =
    translateException(inner.getOptLabelId(labelName))

  override def getPropertyKeyId(propertyKeyName: String): Int =
    translateException(inner.getPropertyKeyId(propertyKeyName))

  override def getPropertyKeyName(id: Int): String =
    translateException(inner.getPropertyKeyName(id))

  override def getLabelId(labelName: String): Int =
    translateException(inner.getLabelId(labelName))

  override def notificationLogger(): InternalNotificationLogger =
    translateException(inner.notificationLogger())

  override def twoLayerTransactionState(): Boolean =
    translateException(inner.twoLayerTransactionState())
}
