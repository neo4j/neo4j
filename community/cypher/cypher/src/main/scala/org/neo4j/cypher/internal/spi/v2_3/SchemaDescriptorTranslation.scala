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
package org.neo4j.cypher.internal.spi.v2_3

import org.neo4j.cypher.internal.compiler.v2_3.spi.SchemaTypes
import org.neo4j.kernel.api.schema_new.constaints.{NodeExistenceConstraintDescriptor, RelExistenceConstraintDescriptor, UniquenessConstraintDescriptor}
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptorFactory
import org.neo4j.kernel.api.schema_new.index.{NewIndexDescriptor => KernelIndexDescriptor}

trait SchemaDescriptorTranslation {
  implicit def cypherToKernel(index: SchemaTypes.IndexDescriptor): KernelIndexDescriptor =
    NewIndexDescriptorFactory.forLabel(index.labelId, index.propertyId)

  implicit def kernelToCypher(index: KernelIndexDescriptor): SchemaTypes.IndexDescriptor =
    if (index.schema().getPropertyIds.length == 1)
      SchemaTypes.IndexDescriptor(index.schema().getLabelId, index.schema().getPropertyIds()(0))
    else
      throw new UnsupportedOperationException("Cypher 2.3 does not support composite indexes")

  implicit def kernelToCypher(index: UniquenessConstraintDescriptor): SchemaTypes.UniquenessConstraint =
    if (index.schema().getPropertyIds.length == 1)
      SchemaTypes.UniquenessConstraint(index.schema().getLabelId, index.schema().getPropertyIds()(0))
    else
      throw new UnsupportedOperationException("Cypher 2.3 does not support composite constraints")

  implicit def kernelToCypher(index: NodeExistenceConstraintDescriptor): SchemaTypes.NodePropertyExistenceConstraint =
    if (index.schema().getPropertyIds.length == 1)
      SchemaTypes.NodePropertyExistenceConstraint(index.schema().getLabelId, index.schema().getPropertyIds()(0))
    else
      throw new UnsupportedOperationException("Cypher 2.3 does not support composite constraints")

  implicit def kernelToCypher(index: RelExistenceConstraintDescriptor): SchemaTypes.RelationshipPropertyExistenceConstraint =
    if (index.schema().getPropertyIds.length == 1)
      SchemaTypes.RelationshipPropertyExistenceConstraint(index.schema().getRelTypeId, index.schema().getPropertyIds()(0))
    else
      throw new UnsupportedOperationException("Cypher 2.3 does not support composite constraints")
}
