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
package org.neo4j.cypher.internal.runtime

import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.v4_0.expressions.LabelToken
import org.neo4j.internal.kernel.api.SchemaRead
import org.neo4j.internal.schema.IndexDescriptor2

import scala.collection.mutable.ArrayBuffer

/**
  * Helper class used to collect the indexes for a query and allocate query-local index ids.
  *
  * @param schemaRead SchemaRead used to acquire index references for registered indexes.
  */
class QueryIndexes(schemaRead: SchemaRead) {

  private val buffer = new ArrayBuffer[InternalIndexReference]
  private var labelScan: Boolean = false

  def registerLabelScan(): Unit = labelScan = true

  def registerQueryIndex(label: LabelToken, property: IndexedProperty): Int = registerQueryIndex(label, Seq(property))

  def registerQueryIndex(label: LabelToken,
                         properties: Seq[IndexedProperty]): Int = {
    val reference = InternalIndexReference(label.nameId.id, properties.map(_.propertyKeyToken.nameId.id))
    val index = buffer.indexOf(reference)
    if ( index > 0 ) index
    else {
      val queryIndexId = buffer.size
      buffer += reference
      queryIndexId
    }
  }

  def indexes: Array[IndexDescriptor2] = buffer.map(index => schemaRead.index(index.label, index.properties:_*)).toArray

  def hasLabelScan: Boolean = labelScan

  private case class InternalIndexReference(label: Int, properties: Seq[Int])
}
