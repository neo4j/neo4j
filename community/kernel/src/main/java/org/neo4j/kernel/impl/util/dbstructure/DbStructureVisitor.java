/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.util.dbstructure;

import org.neo4j.kernel.api.constraints.NodePropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.index.IndexDescriptor;

public interface DbStructureVisitor
{
    void visitLabel( int labelId, String labelName );
    void visitPropertyKey( int propertyKeyId, String propertyKeyName );
    void visitRelationshipType( int relTypeId, String relTypeName );

    void visitIndex( IndexDescriptor descriptor, String userDescription, double uniqueValuesPercentage, long size );
    void visitUniqueIndex( IndexDescriptor descriptor, String userDescription, double uniqueValuesPercentage, long size );
    void visitUniqueConstraint( UniquenessConstraint constraint, String userDescription );
    void visitNodePropertyExistenceConstraint( NodePropertyExistenceConstraint constraint, String userDescription );
    void visitRelationshipPropertyExistenceConstraint( RelationshipPropertyExistenceConstraint constraint,
            String userDescription );

    void visitAllNodesCount( long nodeCount );
    void visitNodeCount( int labelId, String labelName, long nodeCount );
    void visitRelCount( int startLabelId, int relTypeId, int endLabelId, String relCountQuery, long relCount );
}
