/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.spi.v3_3

import org.neo4j.cypher.internal.compiler.v3_3.IndexDescriptor
import org.neo4j.cypher.internal.compiler.v3_3.spi._
import org.neo4j.cypher.internal.frontend.v3_3.phases.InternalNotificationLogger
import org.neo4j.cypher.internal.v3_3.logical.plans.{ProcedureSignature, QualifiedName, UserFunctionSignature}

class ExceptionTranslatingPlanContext(inner: PlanContext) extends PlanContext with ExceptionTranslationSupport {

  override def indexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] =
    translateException(inner.indexesGetForLabel(labelId))

  override def indexGet(labelName: String, propertyKeys: Seq[String]): Option[IndexDescriptor] =
    translateException(inner.indexGet(labelName, propertyKeys))

  override def uniqueIndexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] =
    translateException(inner.uniqueIndexesGetForLabel(labelId))

  override def uniqueIndexGet(labelName: String, propertyKeys: Seq[String]): Option[IndexDescriptor] =
    translateException(inner.uniqueIndexGet(labelName, propertyKeys))

  override def statistics: GraphStatistics =
    translateException(inner.statistics)

  override def checkNodeIndex(idxName: String): Unit =
    translateException(inner.checkNodeIndex(idxName))

  // This should never be used in 3.4 code, because the txIdProvider will be used from 3.4 context in v3_3/Compatibility
  override def txIdProvider: () => Long = ???

  override def procedureSignature(name: QualifiedName): ProcedureSignature =
    translateException(inner.procedureSignature(name))

  override def functionSignature(name: QualifiedName): Option[UserFunctionSignature] =
    translateException(inner.functionSignature(name))

  override def indexExistsForLabel(labelName: String): Boolean =
    translateException(inner.indexExistsForLabel(labelName))

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
}
