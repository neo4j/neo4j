/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import java.util.Map;
import java.util.TreeMap;

import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;

class PropertyStats extends RecordStore.Processor<RuntimeException>
{
    private final Map<Integer,Long> sizeHistogram = new TreeMap<>();
    private final Map<PropertyType,Long> typeHistogram = new EnumMap<>( PropertyType.class );

    @Override
    public void processProperty( RecordStore<PropertyRecord> store, PropertyRecord property )
    {
        int size = 0;
        for ( PropertyBlock block : property )
        {
            update( typeHistogram, block.getType() );
            size++;
        }
        update( sizeHistogram, size );
    }

    private <T> void update( Map<T,Long> histogram, T key )
    {
        Long value = histogram.get( key );
        histogram.put( key, (value == null) ? 1 : (value + 1) );
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder( getClass().getSimpleName() ).append( "{\n" );
        for ( Map.Entry<Integer,Long> entry : sizeHistogram.entrySet() )
        {
            builder.append( '\t' ).append( entry.getKey() ).append( ": " ).append( entry.getValue() ).append(
                    '\n' );
        }
        for ( Map.Entry<PropertyType,Long> entry : typeHistogram.entrySet() )
        {
            builder.append( '\t' ).append( entry.getKey() ).append( ": " ).append( entry.getValue() ).append(
                    '\n' );
        }
        return builder.append( '}' ).toString();
    }

    @Override
    public void processSchema( RecordStore<DynamicRecord> store, DynamicRecord schema ) throws RuntimeException
    {
    }

    @Override
    public void processNode( RecordStore<NodeRecord> store, NodeRecord node ) throws RuntimeException
    {
    }

    @Override
    public void processRelationship( RecordStore<RelationshipRecord> store, RelationshipRecord rel )
            throws RuntimeException
    {
    }


    @Override
    public void processString( RecordStore<DynamicRecord> store, DynamicRecord string, IdType idType )
            throws RuntimeException
    {
    }

    @Override
    public void processArray( RecordStore<DynamicRecord> store, DynamicRecord array ) throws RuntimeException
    {
    }

    @Override
    public void processLabelArrayWithOwner( RecordStore<DynamicRecord> store, DynamicRecord labelArray )
            throws RuntimeException
    {
    }

    @Override
    public void processRelationshipTypeToken( RecordStore<RelationshipTypeTokenRecord> store,
            RelationshipTypeTokenRecord record ) throws RuntimeException
    {
    }

    @Override
    public void processPropertyKeyToken( RecordStore<PropertyKeyTokenRecord> store, PropertyKeyTokenRecord record )
            throws RuntimeException
    {
    }

    @Override
    public void processLabelToken( RecordStore<LabelTokenRecord> store, LabelTokenRecord record )
            throws RuntimeException
    {
    }

    @Override
    public void processRelationshipGroup( RecordStore<RelationshipGroupRecord> store, RelationshipGroupRecord record )
            throws RuntimeException
    {
    }
}
