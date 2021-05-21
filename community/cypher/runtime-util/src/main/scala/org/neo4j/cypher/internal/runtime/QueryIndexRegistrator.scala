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
package org.neo4j.cypher.internal.runtime

import org.neo4j.common.EntityType
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.RelationshipTypeToken
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.NameId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.internal.helpers.collection.Iterators
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.internal.kernel.api.SchemaRead
import org.neo4j.internal.kernel.api.TokenReadSession
import org.neo4j.internal.schema.IndexDescriptor
import org.neo4j.internal.schema.SchemaDescriptor

import scala.collection.mutable.ArrayBuffer

/**
 * Helper class used to register the indexes for a query and allocate query-local index ids.
 *
 * @param schemaRead SchemaRead used to acquire index references for registered indexes.
 */
class QueryIndexRegistrator(schemaRead: SchemaRead) {

  private val indexReferences = new ArrayBuffer[InternalIndexReference]
  private var labelScan: Boolean = false
  private var typeScan: Boolean = false

  def registerLabelScan(): Unit = labelScan = true
  def registerTypeScan(): Unit = typeScan = true

  def registerQueryIndex(label: LabelToken, property: IndexedProperty): Int = registerQueryIndex(label, Seq(property))

  def registerQueryIndex(label: LabelToken,
                         properties: Seq[IndexedProperty]): Int = {
    val reference = InternalIndexReference(label.nameId, properties.map(_.propertyKeyToken.nameId.id))
    val index = indexReferences.indexOf(reference)
    if (index > 0) {
      index
    } else {
      val queryIndexId = indexReferences.size
      indexReferences += reference
      queryIndexId
    }
  }

  def registerQueryIndex(typeToken: RelationshipTypeToken, property: IndexedProperty): Int = registerQueryIndex(typeToken, Seq(property))

  def registerQueryIndex(typeToken: RelationshipTypeToken,
                         properties: Seq[IndexedProperty]): Int = {
    val reference = InternalIndexReference(typeToken.nameId, properties.map(_.propertyKeyToken.nameId.id))
    val index = indexReferences.indexOf(reference)
    if (index > 0) {
      index
    } else {
      val queryIndexId = indexReferences.size
      indexReferences += reference
      queryIndexId
    }
  }

  def result(): QueryIndexes = {
    // We need to use firstOrNull because the indexes might have have been dropped while creating the plan
    val indexes =
      indexReferences.map {
        case InternalIndexReference(LabelId(token), properties) =>
          Iterators.first(schemaRead.indexForSchemaNonTransactional(SchemaDescriptor.forLabel(token, properties: _*)))
        case InternalIndexReference(RelTypeId(token), properties) =>
          Iterators.firstOrNull(schemaRead.indexForSchemaNonTransactional(SchemaDescriptor.forRelType(token, properties: _*)))
        case _ => throw new IllegalStateException()
      }.toArray

    val labelTokenIndex = if (labelScan) {
      Option(Iterators.firstOrNull(schemaRead.indexForSchemaNonTransactional(SchemaDescriptor.forAnyEntityTokens(EntityType.NODE))))
    } else None

    val typeTokenIndex = if (typeScan) {
      Option(Iterators.firstOrNull(schemaRead.indexForSchemaNonTransactional(SchemaDescriptor.forAnyEntityTokens(EntityType.RELATIONSHIP))))
    } else None

    QueryIndexes(indexes, labelTokenIndex, typeTokenIndex)
  }

  private case class InternalIndexReference(token: NameId, properties: Seq[Int])
  private case class InternalTokenReference(token: NameId)
}

case class QueryIndexes(private val indexes: Array[IndexDescriptor],
                        private val labelTokenIndex: Option[IndexDescriptor],
                        private val typeTokenIndex: Option[IndexDescriptor]) {
  def initiateLabelAndSchemaIndexes(queryContext: QueryContext): Array[IndexReadSession] = {
    val indexReadSessions = new Array[IndexReadSession](indexes.length)
    var i = 0
    while (i < indexReadSessions.length) {
      indexReadSessions(i) = queryContext.transactionalContext.dataRead.indexReadSession(indexes(i))
      i += 1
    }
    indexReadSessions
  }

  def initiateNodeTokenIndex(queryContext: QueryContext): Option[TokenReadSession] = {
    labelTokenIndex.map(queryContext.transactionalContext.dataRead.tokenReadSession)
  }

  def initiateRelationshipTokenIndex(queryContext: QueryContext): Option[TokenReadSession] = {
      typeTokenIndex.map(queryContext.transactionalContext.dataRead.tokenReadSession)
  }
}
