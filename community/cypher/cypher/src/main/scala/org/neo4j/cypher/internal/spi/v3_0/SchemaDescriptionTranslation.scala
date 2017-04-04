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
package org.neo4j.cypher.internal.spi.v3_0

import org.neo4j.cypher.internal.compiler.v3_0.spi.SchemaTypes
import org.neo4j.kernel.api.constraints.{
  RelationshipPropertyExistenceConstraint => KernelRPEC,
  NodePropertyExistenceConstraint => KernelNPEC,
  UniquenessConstraint => KernelUniquenessConstraint}
import org.neo4j.kernel.api.index.{IndexDescriptor => KernelIndexDescriptor}

trait SchemaDescriptionTranslation {
  implicit def toKernel( index: SchemaTypes.IndexDescriptor ): KernelIndexDescriptor =
    new KernelIndexDescriptor(index.labelId, index.propertyId)

  implicit def toCypher( index: KernelIndexDescriptor ): SchemaTypes.IndexDescriptor =
    SchemaTypes.IndexDescriptor(index.getLabelId, index.getPropertyKeyId)

  implicit def toKernel( constraint: SchemaTypes.UniquenessConstraint ): KernelUniquenessConstraint =
    new KernelUniquenessConstraint(constraint.labelId, constraint.propertyId)

  implicit def toCypher( constraint: KernelUniquenessConstraint ): SchemaTypes.UniquenessConstraint =
    SchemaTypes.UniquenessConstraint(constraint.label, constraint.propertyKey)

  implicit def toKernel( constraint: SchemaTypes.NodePropertyExistenceConstraint ): KernelNPEC =
    new KernelNPEC(constraint.labelId, constraint.propertyId)

  implicit def toCypher( constraint: KernelNPEC ): SchemaTypes.NodePropertyExistenceConstraint =
    SchemaTypes.NodePropertyExistenceConstraint(constraint.label, constraint.propertyKey)

  implicit def toKernel( constraint: SchemaTypes.RelationshipPropertyExistenceConstraint ): KernelRPEC =
    new KernelRPEC(constraint.relTypeId, constraint.propertyId)

  implicit def toCypher( constraint: KernelRPEC ): SchemaTypes.RelationshipPropertyExistenceConstraint =
    SchemaTypes.RelationshipPropertyExistenceConstraint(constraint.relationshipType, constraint.propertyKey)

}
