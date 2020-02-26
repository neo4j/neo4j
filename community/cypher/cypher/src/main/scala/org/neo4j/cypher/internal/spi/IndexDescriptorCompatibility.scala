/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.cypher.internal.planner.spi.IndexBehaviour
import org.neo4j.cypher.internal.planner.spi.SkipAndLimit
import org.neo4j.cypher.internal.planner.spi.SlowContains
import org.neo4j.cypher.internal.planner.spi
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundTokenContext
import org.neo4j.internal.schema
import org.neo4j.internal.schema.LabelSchemaDescriptor
import org.neo4j.internal.schema.SchemaDescriptor

trait IndexDescriptorCompatibility {
  def kernelToCypher(behaviour: schema.IndexBehaviour): IndexBehaviour = {
    behaviour match {
      case schema.IndexBehaviour.SLOW_CONTAINS => SlowContains
      case schema.IndexBehaviour.SKIP_AND_LIMIT => SkipAndLimit
      case _ => throw new IllegalStateException("Missing kernel to cypher mapping for index behaviour: " + behaviour)
    }
  }

  def cypherToKernelSchema(index: spi.IndexDescriptor): LabelSchemaDescriptor =
    SchemaDescriptor.forLabel(index.label.id, index.properties.map(_.id):_*)

  def toLabelSchemaDescriptor(labelId: Int, propertyKeyIds: Seq[Int]): LabelSchemaDescriptor =
    SchemaDescriptor.forLabel(labelId, propertyKeyIds.toArray:_*)

  def toLabelSchemaDescriptor(tc: TransactionBoundTokenContext, labelName: String, propertyKeys: Seq[String]): LabelSchemaDescriptor = {
    val labelId: Int = tc.getLabelId(labelName)
    val propertyKeyIds: Seq[Int] = propertyKeys.map(tc.getPropertyKeyId)
    toLabelSchemaDescriptor(labelId, propertyKeyIds)
  }
}
