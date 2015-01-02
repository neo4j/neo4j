/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.perftest.enterprise.generator;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;

class PropertyStats extends RecordStore.Processor<RuntimeException>
{
    Map<Integer, Long> sizeHistogram = new TreeMap<Integer, Long>();
    Map<PropertyType, Long> typeHistogram = new EnumMap<PropertyType, Long>( PropertyType.class );

    @Override
    public void processProperty( RecordStore<PropertyRecord> store, PropertyRecord property )
    {
        List<PropertyBlock> blocks = property.getPropertyBlocks();
        update( sizeHistogram, blocks.size() );
        for ( PropertyBlock block : blocks )
        {
            update( typeHistogram, block.getType() );
        }
    }

    private <T> void update( Map<T, Long> histogram, T key )
    {
        Long value = histogram.get( key );
        histogram.put( key, (value == null) ? 1 : (value + 1) );
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder( getClass().getSimpleName() ).append( "{\n" );
        for ( Map.Entry<Integer, Long> entry : sizeHistogram.entrySet() )
        {
            builder.append( '\t' ).append( entry.getKey() ).append( ": " ).append( entry.getValue() ).append(
                    '\n' );
        }
        for ( Map.Entry<PropertyType, Long> entry : typeHistogram.entrySet() )
        {
            builder.append( '\t' ).append( entry.getKey() ).append( ": " ).append( entry.getValue() ).append(
                    '\n' );
        }
        return builder.append( '}' ).toString();
    }
}
