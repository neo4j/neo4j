/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport;

import java.util.Iterator;

import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.xa.PropertyCreator;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository;

/**
 * Common and cross-concern utilities.
 */
public class Utils
{
    public static long[] labelNamesToIds( BatchingTokenRepository<?> labelHolder, String[] labels )
    {
        long[] result = new long[labels.length];
        for ( int i = 0; i < labels.length; i++ )
        {
            result[i] = labelHolder.getOrCreateId( labels[i] );
        }
        return result;
    }

    public static Iterator<PropertyBlock> propertyKeysAndValues( final Object[] properties,
            final BatchingTokenRepository<?> propertyKeyHolder, final PropertyCreator creator )
    {
        return new PrefetchingIterator<PropertyBlock>()
        {
            private int cursor;

            @Override
            protected PropertyBlock fetchNextOrNull()
            {
                if ( cursor >= properties.length )
                {
                    return null;
                }

                int key = propertyKeyHolder.getOrCreateId( (String)properties[cursor++] );
                Object value = properties[cursor++];
                return creator.encodeValue( new PropertyBlock(), key, value );
            }
        };
    }

    public static int safeCastLongToInt( long value )
    {
        if ( value > Integer.MAX_VALUE )
        {
            throw new UnsupportedOperationException( "Not supported a.t.m" );
        }
        return (int) value;
    }

    private Utils()
    {   // No instances allowed
    }
}
