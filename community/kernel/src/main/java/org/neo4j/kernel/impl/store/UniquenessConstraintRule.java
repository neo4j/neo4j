/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.store;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.neo4j.kernel.impl.store.record.AbstractSchemaRule;

import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.safeCastLongToInt;

public class UniquenessConstraintRule extends AbstractSchemaRule
{
    private final int[] propertyKeyIds;
    private final long ownedIndexRule;

    /** We currently only support uniqueness constraints on a single property. */
    public static UniquenessConstraintRule uniquenessConstraintRule( long id, int labelId, int propertyKeyId,
                                                                     long ownedIndexRule )
    {
        return new UniquenessConstraintRule( id, labelId, new int[] {propertyKeyId}, ownedIndexRule );
    }

    public static UniquenessConstraintRule readUniquenessConstraintRule( long id, int labelId, ByteBuffer buffer )
    {
        return new UniquenessConstraintRule( id, labelId, readPropertyKeys( buffer ), readOwnedIndexRule( buffer ) );
    }

    private UniquenessConstraintRule( long id, int labelId, int[] propertyKeyIds, long ownedIndexRule )
    {
        super( id, labelId, Kind.UNIQUENESS_CONSTRAINT );
        this.ownedIndexRule = ownedIndexRule;
        assert propertyKeyIds.length == 1; // Only uniqueness of a single property supported for now
        this.propertyKeyIds = propertyKeyIds;
    }

    @Override
    public int hashCode()
    {
        return super.hashCode() | Arrays.hashCode( propertyKeyIds );
    }

    @Override
    public boolean equals( Object obj )
    {
        return super.equals( obj ) && Arrays.equals( propertyKeyIds, ((UniquenessConstraintRule) obj).propertyKeyIds );
    }

    @Override
    protected String innerToString()
    {
        return ", propertyKeys=" + Arrays.toString( propertyKeyIds ) + ", ownedIndex=" + ownedIndexRule;
    }

    @Override
    public int length()
    {
        return super.length() +
               1 +  /* the number of properties that form a unique tuple */
               8 * propertyKeyIds.length + /* the property keys themselves */
               8; /* owned index rule */
    }

    @Override
    public void serialize( ByteBuffer target )
    {
        super.serialize( target );
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
}
