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

import org.neo4j.kernel.api.index.{IndexDescriptor => KernelIndexDescriptor}
import org.neo4j.cypher.internal.compiler.v2_3.spi.SchemaTypes
import org.neo4j.kernel.api.constraints.{NodePropertyExistenceConstraint, RelationshipPropertyConstraint, UniquenessConstraint}

trait SchemaDescriptorTranslation {
  implicit def cypherToKernel(index:SchemaTypes.IndexDescriptor):KernelIndexDescriptor =
    new KernelIndexDescriptor( index.labelId, index.propertyId )

  implicit def kernelToCypher(index:KernelIndexDescriptor):SchemaTypes.IndexDescriptor =
    SchemaTypes.IndexDescriptor( index.getLabelId, index.getPropertyKeyId )

  implicit def kernelToCypher(constraint:UniquenessConstraint):SchemaTypes.UniquenessConstraint =
    SchemaTypes.UniquenessConstraint( constraint.label(), constraint.propertyKey() )

  implicit def kernelToCypher(constraint:NodePropertyExistenceConstraint):SchemaTypes.NodePropertyExistenceConstraint =
    SchemaTypes.NodePropertyExistenceConstraint( constraint.label(), constraint.propertyKey() )

  implicit def kernelToCypher(constraint:RelationshipPropertyConstraint):SchemaTypes.RelationshipPropertyExistenceConstraint =
    SchemaTypes.RelationshipPropertyExistenceConstraint( constraint.relationshipType(), constraint.propertyKey() )
}
