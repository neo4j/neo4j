/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.neo4j.cypher.internal.compiler.v3_4.{IndexDescriptor => CypherIndexDescriptor}
import org.neo4j.kernel.api.schema.index.{IndexDescriptorFactory, IndexDescriptor => KernelIndexDescriptor}
import org.neo4j.kernel.api.schema.{LabelSchemaDescriptor, SchemaDescriptorFactory}

trait IndexDescriptorCompatibility {
  implicit def cypherToKernel(index: CypherIndexDescriptor): KernelIndexDescriptor =
    IndexDescriptorFactory.forLabel(index.label.id, index.properties.map(_.id):_*)

  implicit def kernelToCypher(index: KernelIndexDescriptor): CypherIndexDescriptor =
    CypherIndexDescriptor(index.schema().getLabelId, index.schema().getPropertyIds)

  implicit def cypherToKernelSchema(index: CypherIndexDescriptor): LabelSchemaDescriptor =
    SchemaDescriptorFactory.forLabel(index.label.id, index.properties.map(_.id):_*)

  implicit def toLabelSchemaDescriptor(labelId: Int, propertyKeyIds: Seq[Int]): LabelSchemaDescriptor =
      SchemaDescriptorFactory.forLabel(labelId, propertyKeyIds.toArray:_*)

  implicit def toLabelSchemaDescriptor(tc: TransactionBoundTokenContext, labelName: String, propertyKeys: Seq[String]): LabelSchemaDescriptor = {
    val labelId: Int = tc.getLabelId(labelName)
    val propertyKeyIds: Seq[Int] = propertyKeys.map(tc.getPropertyKeyId)
    toLabelSchemaDescriptor(labelId, propertyKeyIds)
  }
}
