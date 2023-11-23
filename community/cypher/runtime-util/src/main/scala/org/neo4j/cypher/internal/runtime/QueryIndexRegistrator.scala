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
package org.neo4j.cypher.internal.runtime

import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.RelationshipTypeToken
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.NameId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.graphdb.schema.IndexType
import org.neo4j.internal
import org.neo4j.internal.helpers.collection.Iterators
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.internal.kernel.api.SchemaRead
import org.neo4j.internal.kernel.api.TokenReadSession
import org.neo4j.internal.schema.IndexDescriptor
import org.neo4j.internal.schema.SchemaDescriptors

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

  def registerQueryIndex(indexType: IndexType, label: LabelToken, property: IndexedProperty): Int =
    registerQueryIndex(indexType, label, Seq(property))

  def registerQueryIndex(indexType: IndexType, label: LabelToken, properties: collection.Seq[IndexedProperty]): Int =
    registerQueryIndex(indexType, label.nameId, properties)

  def registerQueryIndex(indexType: IndexType, typeToken: RelationshipTypeToken, property: IndexedProperty): Int =
    registerQueryIndex(indexType, typeToken, Seq(property))

  def registerQueryIndex(
    indexType: IndexType,
    relationshipTypeToken: RelationshipTypeToken,
    properties: collection.Seq[IndexedProperty]
  ): Int =
    registerQueryIndex(indexType, relationshipTypeToken.nameId, properties)

  private def registerQueryIndex(
    indexType: IndexType,
    tokenNameId: NameId,
    properties: collection.Seq[IndexedProperty]
  ): Int = {
    val reference = InternalIndexReference(
      tokenNameId,
      properties.map(_.propertyKeyToken.nameId.id),
      internal.schema.IndexType.fromPublicApi(indexType)
    )
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
    val indexes =
      indexReferences.map {
        case InternalIndexReference(LabelId(token), properties, indexType) =>
          schemaRead.indexForSchemaAndIndexTypeNonTransactional(
            SchemaDescriptors.forLabel(token, properties.toSeq: _*),
            indexType
          )

        case InternalIndexReference(RelTypeId(token), properties, indexType) =>
          schemaRead.indexForSchemaAndIndexTypeNonTransactional(
            SchemaDescriptors.forRelType(token, properties.toSeq: _*),
            indexType
          )

        case _ => throw new IllegalStateException()
      }.toArray

    val labelTokenIndex =
      if (labelScan) {
        // We need to use firstOrNull because the indexes might have have been dropped while creating the plan
        Option(Iterators.firstOrNull(
          schemaRead.indexForSchemaNonTransactional(SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR)
        ))
      } else None

    val typeTokenIndex =
      if (typeScan) {
        // We need to use firstOrNull because the indexes might have have been dropped while creating the plan
        Option(Iterators.firstOrNull(
          schemaRead.indexForSchemaNonTransactional(SchemaDescriptors.ANY_TOKEN_RELATIONSHIP_SCHEMA_DESCRIPTOR)
        ))
      } else None

    QueryIndexes(indexes, labelTokenIndex, typeTokenIndex)
  }

  private case class InternalIndexReference(
    token: NameId,
    properties: collection.Seq[Int],
    indexType: internal.schema.IndexType
  )
  private case class InternalTokenReference(token: NameId)
}

case class QueryIndexes(
  private val indexes: Array[IndexDescriptor],
  private val labelTokenIndex: Option[IndexDescriptor],
  private val typeTokenIndex: Option[IndexDescriptor]
) {

  def initiateLabelAndSchemaIndexes(queryContext: ReadQueryContext): Array[IndexReadSession] = {
    val indexReadSessions = new Array[IndexReadSession](indexes.length)
    var i = 0
    while (i < indexReadSessions.length) {
      indexReadSessions(i) = queryContext.transactionalContext.dataRead.indexReadSession(indexes(i))
      i += 1
    }
    indexReadSessions
  }

  def initiateNodeTokenIndex(queryContext: ReadQueryContext): Option[TokenReadSession] = {
    labelTokenIndex.map(queryContext.transactionalContext.dataRead.tokenReadSession)
  }

  def initiateRelationshipTokenIndex(queryContext: ReadQueryContext): Option[TokenReadSession] = {
    typeTokenIndex.map(queryContext.transactionalContext.dataRead.tokenReadSession)
  }
}
