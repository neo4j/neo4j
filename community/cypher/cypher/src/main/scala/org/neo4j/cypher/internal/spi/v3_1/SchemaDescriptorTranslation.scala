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
package org.neo4j.cypher.internal.spi.v3_1

import org.neo4j.cypher.internal.compiler.v3_1.spi.SchemaTypes
import org.neo4j.internal.kernel.api.{IndexReference => KernelIndexReference}
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor
import org.neo4j.kernel.api.schema.constraints.{ConstraintDescriptorFactory, NodeExistenceConstraintDescriptor, RelExistenceConstraintDescriptor, UniquenessConstraintDescriptor => KernelUniquenessConstraint}

trait SchemaDescriptorTranslation {
  implicit def toCypher(index: KernelIndexReference): SchemaTypes.IndexDescriptor = {
    assertSingleProperty(index.properties())
      SchemaTypes.IndexDescriptor(index.schema().getEntityTokenIds()(0), index.schema().getPropertyIds()(0))
  }

  implicit def toKernel(constraint: SchemaTypes.UniquenessConstraint): KernelUniquenessConstraint =
    ConstraintDescriptorFactory.uniqueForLabel(constraint.labelId, constraint.propertyId)

  implicit def toCypher(constraint: KernelUniquenessConstraint): SchemaTypes.UniquenessConstraint = {
    assertSingleProperty(constraint.schema())
    SchemaTypes.UniquenessConstraint(constraint.schema().getLabelId, constraint.schema().getPropertyId)
  }

  implicit def toCypher(constraint: NodeExistenceConstraintDescriptor): SchemaTypes.NodePropertyExistenceConstraint = {
    assertSingleProperty(constraint.schema())
    SchemaTypes.NodePropertyExistenceConstraint(constraint.schema().getLabelId, constraint.schema().getPropertyId)
  }

  implicit def toCypher(constraint: RelExistenceConstraintDescriptor): SchemaTypes.RelationshipPropertyExistenceConstraint = {
    assertSingleProperty(constraint.schema())
    SchemaTypes.RelationshipPropertyExistenceConstraint(constraint.schema().getRelTypeId, constraint.schema().getPropertyId)
  }

  def assertSingleProperty(schema: SchemaDescriptor):Unit =
    assertSingleProperty(schema.getPropertyIds)

  def assertSingleProperty(properties: Array[Int]): Unit =
    if (properties.length != 1)
      throw new UnsupportedOperationException("Cypher 3.1 does not support composite indexes or constraints")
}
