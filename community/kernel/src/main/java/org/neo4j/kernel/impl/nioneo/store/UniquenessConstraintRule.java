/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class UniquenessConstraintRule extends AbstractSchemaRule
{
    private final long[] propertyKeyIds;

    /** We currently only support uniqueness constraints on a single property. */
    public UniquenessConstraintRule( long id, long labelId, long propertyKeyId )
    {
        this( id, labelId, new long[]{propertyKeyId} );
    }

    UniquenessConstraintRule( long id, long labelId, ByteBuffer buffer )
    {
        this( id, labelId, readPropertyKeys( buffer ) );
    }

    private UniquenessConstraintRule( long id, long labelId, long[] propertyKeyIds )
    {
        super( id, labelId, Kind.UNIQUENESS_CONSTRAINT );
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
        return ", propertyKeys=" + Arrays.toString( propertyKeyIds );
    }

    @Override
    public int length()
    {
        return super.length() +
               1 +  /* the number of properties that form a unique tuple */
               8 * propertyKeyIds.length /* the property keys themselves */;
    }

    @Override
    public void serialize( ByteBuffer target )
    {
        super.serialize( target );
        target.put( (byte) propertyKeyIds.length );
        for ( long propertyKeyId : propertyKeyIds )
        {
            target.putLong( propertyKeyId );
        }
    }

    private static long[] readPropertyKeys( ByteBuffer buffer )
    {
        long[] keys = new long[buffer.get()];
        for ( int i = 0; i < keys.length; i++ )
        {
            keys[i] = buffer.getLong();
        }
        return keys;
    }

    public boolean containsPropertyKeyId( long propertyKeyId )
    {
        for ( long keyId : propertyKeyIds )
        {
            if (keyId == propertyKeyId) return true;
        }
        return false;
    }
}
