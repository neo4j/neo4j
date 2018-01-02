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
package org.neo4j.kernel.api.constraints;

import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.impl.coreapi.schema.InternalSchemaActions;

public abstract class PropertyConstraint
{
    public interface ChangeVisitor
    {
        void visitAddedUniquePropertyConstraint( UniquenessConstraint constraint );

        void visitRemovedUniquePropertyConstraint( UniquenessConstraint constraint );

        void visitAddedNodePropertyExistenceConstraint( NodePropertyExistenceConstraint constraint )
                throws CreateConstraintFailureException;

        void visitRemovedNodePropertyExistenceConstraint( NodePropertyExistenceConstraint constraint );

        void visitAddedRelationshipPropertyExistenceConstraint( RelationshipPropertyExistenceConstraint constraint )
                throws CreateConstraintFailureException;

        void visitRemovedRelationshipPropertyExistenceConstraint( RelationshipPropertyExistenceConstraint constraint );
    }

    protected final int propertyKeyId;

    public PropertyConstraint( int propertyKeyId )
    {
        this.propertyKeyId = propertyKeyId;
    }

    public abstract void added( ChangeVisitor visitor ) throws CreateConstraintFailureException;

    public abstract void removed( ChangeVisitor visitor );

    public int propertyKey()
    {
        return propertyKeyId;
    }

    public abstract ConstraintType type();

    public abstract String userDescription( TokenNameLookup tokenNameLookup );

    public abstract ConstraintDefinition asConstraintDefinition( InternalSchemaActions schemaActions,
            ReadOperations readOps );

    @Override
    public abstract boolean equals( Object o );

    @Override
    public abstract int hashCode();

    @Override
    public abstract String toString();
}
