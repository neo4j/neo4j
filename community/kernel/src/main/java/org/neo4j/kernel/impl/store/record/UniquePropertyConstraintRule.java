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
package org.neo4j.kernel.impl.store.record;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.neo4j.kernel.api.constraints.UniquenessConstraint;

import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.safeCastLongToInt;

public class UniquePropertyConstraintRule extends NodePropertyConstraintRule
{
    private final int[] propertyKeyIds;
    private final long ownedIndexRule;

    /** We currently only support uniqueness constraints on a single property. */
    public static UniquePropertyConstraintRule uniquenessConstraintRule( long id, int labelId, int propertyKeyId,
                                                                         long ownedIndexRule )
    {
        return new UniquePropertyConstraintRule( id, labelId, new int[] {propertyKeyId}, ownedIndexRule );
    }

    public static UniquePropertyConstraintRule readUniquenessConstraintRule( long id, int labelId, ByteBuffer buffer )
    {
        return new UniquePropertyConstraintRule( id, labelId, readPropertyKeys( buffer ), readOwnedIndexRule( buffer ) );
    }

    private UniquePropertyConstraintRule( long id, int labelId, int[] propertyKeyIds, long ownedIndexRule )
    {
        super( id, labelId, Kind.UNIQUENESS_CONSTRAINT );
        this.ownedIndexRule = ownedIndexRule;
        assert propertyKeyIds.length == 1; // Only uniqueness of a single property supported for now
        this.propertyKeyIds = propertyKeyIds;
    }

    @Override
    public String toString()
    {
        return "UniquePropertyConstraintRule[id=" + id + ", label=" + label + ", kind=" + kind +
               ", propertyKeys=" + Arrays.toString( propertyKeyIds ) + ", ownedIndex=" + ownedIndexRule + "]";
    }

    @Override
    public int length()
    {
        return 4 /* label */ +
               1 /* kind id */ +
               1 +  /* the number of properties that form a unique tuple */
               8 * propertyKeyIds.length + /* the property keys themselves */
               8; /* owned index rule */
    }

    @Override
    public void serialize( ByteBuffer target )
    {
        target.putInt( label );
        target.put( kind.id() );
        target.put( (byte) propertyKeyIds.length );
        for ( int propertyKeyId : propertyKeyIds )
        {
            target.putLong( propertyKeyId );
        }
        target.putLong( ownedIndexRule );
    }

    private static int[] readPropertyKeys( ByteBuffer buffer )
    {
        int[] keys = new int[buffer.get()];
        for ( int i = 0; i < keys.length; i++ )
        {
            keys[i] = safeCastLongToInt( buffer.getLong() );
        }
        return keys;
    }

    private static long readOwnedIndexRule( ByteBuffer buffer )
    {
        return buffer.getLong();
    }

    @Override
    public boolean containsPropertyKeyId( int propertyKeyId )
    {
        for ( int keyId : propertyKeyIds )
        {
            if ( keyId == propertyKeyId )
            {
                return true;
            }
        }
        return false;
    }

    // This method exists as long as only single property keys are supported
    public int getPropertyKey()
    {
        // Property key "singleness" is checked elsewhere, in the constructor and when deserializing.
        return propertyKeyIds[0];
    }

    public long getOwnedIndex()
    {
        return ownedIndexRule;
    }

    @Override
    public UniquenessConstraint toConstraint()
    {
        return new UniquenessConstraint( getLabel(), getPropertyKey() );
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
        return Arrays.equals( propertyKeyIds, ((UniquePropertyConstraintRule) o).propertyKeyIds );
    }

    @Override
    public int hashCode()
    {
        return 31 * super.hashCode() + Arrays.hashCode( propertyKeyIds );
    }
}
