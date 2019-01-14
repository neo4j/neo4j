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
package org.neo4j.kernel.impl.util.dbstructure;

import org.neo4j.kernel.api.schema.constaints.NodeExistenceConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.NodeKeyConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.RelExistenceConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;

public interface DbStructureVisitor
{
    void visitLabel( int labelId, String labelName );
    void visitPropertyKey( int propertyKeyId, String propertyKeyName );
    void visitRelationshipType( int relTypeId, String relTypeName );

    void visitIndex( SchemaIndexDescriptor descriptor, String userDescription, double uniqueValuesPercentage, long size );
    void visitUniqueConstraint( UniquenessConstraintDescriptor constraint, String userDescription );
    void visitNodePropertyExistenceConstraint( NodeExistenceConstraintDescriptor constraint, String userDescription );
    void visitRelationshipPropertyExistenceConstraint( RelExistenceConstraintDescriptor constraint,
            String userDescription );
    void visitNodeKeyConstraint( NodeKeyConstraintDescriptor constraint, String userDescription );

    void visitAllNodesCount( long nodeCount );
    void visitNodeCount( int labelId, String labelName, long nodeCount );
    void visitRelCount( int startLabelId, int relTypeId, int endLabelId, String relCountQuery, long relCount );
}
