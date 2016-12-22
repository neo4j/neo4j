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
package org.neo4j.kernel.impl.store.record;

import java.nio.ByteBuffer;

import org.neo4j.kernel.api.schema.EntityPropertyDescriptor;
import org.neo4j.kernel.api.schema.RelationshipPropertyDescriptor;
import org.neo4j.kernel.api.constraints.RelationshipPropertyConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyExistenceConstraint;

public class RelationshipPropertyExistenceConstraintRule extends RelationshipPropertyConstraintRule
{
    private final RelationshipPropertyDescriptor descriptor;

    public static RelationshipPropertyExistenceConstraintRule relPropertyExistenceConstraintRule( long id,
            RelationshipPropertyDescriptor descriptor )
    {
        return new RelationshipPropertyExistenceConstraintRule( id, descriptor );
    }

    public static RelationshipPropertyExistenceConstraintRule readRelPropertyExistenceConstraintRule( long id,
            int relTypeId, ByteBuffer buffer )
    {
        return new RelationshipPropertyExistenceConstraintRule( id,
                new RelationshipPropertyDescriptor( relTypeId, readPropertyKey( buffer ) ) );
    }

    private RelationshipPropertyExistenceConstraintRule( long id, RelationshipPropertyDescriptor descriptor )
    {
        super( id, descriptor.getRelationshipTypeId(), Kind.RELATIONSHIP_PROPERTY_EXISTENCE_CONSTRAINT );
        this.descriptor = descriptor;
    }

    @Override
    public final EntityPropertyDescriptor descriptor()
    {
        return descriptor;
    }

    @Override
    public String toString()
    {
        return "RelationshipPropertyExistenceConstraint" + id + ", relationshipType=" + relationshipType +
               ", kind=" + kind + ", propertyKeyId=" + descriptor.propertyIdText() + "]";
    }

    @Override
    public int length()
    {
        return 4 /* relationship type id */ +
               1 /* kind id */ +
               4; /* property key id */
    }

    @Override
    public void serialize( ByteBuffer target )
    {
        target.putInt( relationshipType );
        target.put( kind.id() );
        target.putInt( descriptor.getPropertyKeyId() );
    }

    private static int readPropertyKey( ByteBuffer buffer )
    {
        return buffer.getInt();
    }

    public int getPropertyKey()
    {
        return descriptor.getPropertyKeyId();
    }

    @Override
    public RelationshipPropertyConstraint toConstraint()
    {
        return new RelationshipPropertyExistenceConstraint( descriptor );
    }

    @Override
    public boolean containsPropertyKeyId( int propertyKeyId )
    {
        return propertyKeyId == descriptor.getPropertyKeyId();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        if ( !super.equals( o ) )
        {
            return false;
        }
        return descriptor.equals( ((RelationshipPropertyExistenceConstraintRule) o).descriptor );
    }

    @Override
    public int hashCode()
    {
        return 31 * super.hashCode() + descriptor.hashCode();
    }
}
