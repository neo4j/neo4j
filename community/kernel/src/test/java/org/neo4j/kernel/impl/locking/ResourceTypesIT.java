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
package org.neo4j.kernel.impl.locking;

import org.junit.Test;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongLongMap;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.impl.locking.ResourceTypes.indexEntryResourceId;

/**
 * This is an *IT integration test because it uses a large amount of memory.
 * Approximately 1.2 GBs goes into the map we use to check for collisions.
 */
public class ResourceTypesIT
{
    @Test
    public void indexEntryHashing()
    {
        PrimitiveLongLongMap map = Primitive.offHeapLongLongMap( 35_000_000 );
        int collisions = 0;

        int labelIdCount = 50;
        int propertyKeyIdCount = 50;
        int objectCount = 10000;
        String[] values = precomputeValues( objectCount );

        for ( int labelId = 0; labelId < labelIdCount; labelId++ )
        for ( int propertyKeyId = 0; propertyKeyId < propertyKeyIdCount; propertyKeyId++ )
        for ( int objectId = 0; objectId < objectCount; objectId++ )
        {
            String object = values[objectId];
            long resourceId = indexEntryResourceId( labelId, propertyKeyId, object );

            long newValue = packValue( labelId, propertyKeyId, objectId );
            long oldValue = map.put( resourceId, newValue );
            if ( oldValue != -1 )
            {
                System.out.printf( "Collision on %s: %s ~= %s%n",
                        resourceId, toValueString( newValue ), toValueString( oldValue ) );
                collisions++;
                if ( collisions > 100 )
                {
                    fail( "This hashing is terrible!" );
                }
            }
        }

        assertThat( collisions, is( 0 ) );
    }

    private long packValue( int labelId, int propertyKeyId, int objectId )
    {
        long result = labelId;
        result <<= 16;
        result += propertyKeyId;
        result <<= 32;
        result += objectId;
        return result;
    }

    private String toValueString( long value )
    {
        int objectId = (int) (value & 0x00000000_FFFFFFFFL);
        int propertyKeyId = (int) ((value & 0x0000FFFF_00000000L) >>> 32);
        int labelId = (int) ((value & 0xFFFF0000_00000000L) >>> 48);
        return String.format( "IndexEntry{ labelId=%s, propertyKeyId=%s, objectId=%s }",
                labelId, propertyKeyId, objectId );
    }

    private String[] precomputeValues( int objectCount )
    {
        String[] values = new String[objectCount];
        for ( int i = 0; i < objectCount; i++ )
        {
            values[i] = "" + i;
        }
        return values;
    }
}
