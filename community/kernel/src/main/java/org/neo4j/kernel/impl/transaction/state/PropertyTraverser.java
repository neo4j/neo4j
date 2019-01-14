/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.transaction.state;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.util.Listener;

public class PropertyTraverser
{
    /**
     * Traverses a property record chain and finds the record containing the property with key {@code propertyKey}.
     * If none is found and {@code strict} is {@code true} then {@link IllegalStateException} is thrown,
     * otherwise id value of {@link Record#NO_NEXT_PROPERTY} is returned.
     *
     * @param primitive {@link PrimitiveRecord} which is the owner of the chain.
     * @param propertyKey property key token id to look for.
     * @param propertyRecords access to records.
     * @param strict dictates behavior on property key not found. If {@code true} then {@link IllegalStateException}
     * is thrown, otherwise value of {@link Record#NO_NEXT_PROPERTY} is returned.
     * @return property record id containing property with the given {@code propertyKey}, otherwise if
     * {@code strict} is false value of {@link Record#NO_NEXT_PROPERTY}.
     */
    public long findPropertyRecordContaining( PrimitiveRecord primitive, int propertyKey,
            RecordAccess<PropertyRecord, PrimitiveRecord> propertyRecords, boolean strict )
    {
        long propertyRecordId = primitive.getNextProp();
        while ( !Record.NO_NEXT_PROPERTY.is( propertyRecordId ) )
        {
            PropertyRecord propertyRecord =
                    propertyRecords.getOrLoad( propertyRecordId, primitive ).forReadingLinkage();
            if ( propertyRecord.getPropertyBlock( propertyKey ) != null )
            {
                return propertyRecordId;
            }
            propertyRecordId = propertyRecord.getNextProp();
        }

        if ( strict )
        {
            throw new IllegalStateException( "No property record in property chain for " + primitive +
                    " contained property with key " + propertyKey );
        }

        return Record.NO_NEXT_PROPERTY.intValue();
    }

    public void getPropertyChain( long nextProp,
            RecordAccess<PropertyRecord, PrimitiveRecord> propertyRecords,
            Listener<PropertyBlock> collector )
    {
        while ( nextProp != Record.NO_NEXT_PROPERTY.intValue() )
        {
            PropertyRecord propRecord = propertyRecords.getOrLoad( nextProp, null ).forReadingData();
            for ( PropertyBlock propBlock : propRecord )
            {
                collector.receive( propBlock );
            }
            nextProp = propRecord.getNextProp();
        }
    }

    public boolean assertPropertyChain( PrimitiveRecord primitive,
            RecordAccess<PropertyRecord, PrimitiveRecord> propertyRecords )
    {
        List<PropertyRecord> toCheck = new LinkedList<>();
        long nextIdToFetch = primitive.getNextProp();
        while ( nextIdToFetch != Record.NO_NEXT_PROPERTY.intValue() )
        {
            PropertyRecord propRecord = propertyRecords.getOrLoad( nextIdToFetch, primitive ).forReadingLinkage();
            toCheck.add( propRecord );
            assert propRecord.inUse() : primitive + "->"
                                        + Arrays.toString( toCheck.toArray() );
            assert propRecord.size() <= PropertyType.getPayloadSize() : propRecord + " size " + propRecord.size();
            nextIdToFetch = propRecord.getNextProp();
        }
        if ( toCheck.isEmpty() )
        {
            assert primitive.getNextProp() == Record.NO_NEXT_PROPERTY.intValue() : primitive;
            return true;
        }
        PropertyRecord first = toCheck.get( 0 );
        PropertyRecord last = toCheck.get( toCheck.size() - 1 );
        assert first.getPrevProp() == Record.NO_PREVIOUS_PROPERTY.intValue() : primitive
                                                                               + "->"
                                                                               + Arrays.toString( toCheck.toArray() );
        assert last.getNextProp() == Record.NO_NEXT_PROPERTY.intValue() : primitive
                                                                          + "->"
                                                                          + Arrays.toString( toCheck.toArray() );
        PropertyRecord current;
        PropertyRecord previous = first;
        for ( int i = 1; i < toCheck.size(); i++ )
        {
            current = toCheck.get( i );
            assert current.getPrevProp() == previous.getId() : primitive
                                                               + "->"
                                                               + Arrays.toString( toCheck.toArray() );
            assert previous.getNextProp() == current.getId() : primitive
                                                               + "->"
                                                               + Arrays.toString( toCheck.toArray() );
            previous = current;
        }
        return true;
    }
}
