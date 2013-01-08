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
package org.neo4j.kernel.impl.storemigration;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.helpers.Pair;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.Record;

public class PropertyWriter
{
    private PropertyStore propertyStore;

    public PropertyWriter( PropertyStore propertyStore )
    {
        this.propertyStore = propertyStore;
    }

    /**
     * Transforms a mapping of key index ids to values into a property chain.
     * Mostly copied-pasted from BatchTransactionImpl.
     *
     * @param properties The mapping, as a list of Pairs of keys and values
     * @return a long value valid for a property record id that is the id of the
     *         head of the chain, suitable for a nextProp() value on a
     *         primitive.
     */
    public long writeProperties( List<Pair<Integer, Object>> properties )
    {
        if ( properties == null || properties.isEmpty() )
        {
            return Record.NO_NEXT_PROPERTY.intValue();
        }
        // To hold the records, we will write them out in reverse order
        List<PropertyRecord> propRecords = new ArrayList<PropertyRecord>();
        // There is at least one property, so we will create at least one record.
        PropertyRecord currentRecord = new PropertyRecord( propertyStore.nextId() );
        currentRecord.setInUse( true );
        currentRecord.setCreated();
        propRecords.add( currentRecord );

        for ( Pair<Integer, Object> propertyDatum : properties )
        {
            PropertyBlock block = new PropertyBlock();
            propertyStore.encodeValue( block, propertyDatum.first(),
                    propertyDatum.other() );
            if ( currentRecord.size() + block.getSize() > PropertyType.getPayloadSize() )
            {
                // Here it means the current block is done for
                PropertyRecord prevRecord = currentRecord;
                // Create new record
                long propertyId = propertyStore.nextId();
                currentRecord = new PropertyRecord( propertyId );
                currentRecord.setInUse( true );
                currentRecord.setCreated();
                // Set up links
                prevRecord.setNextProp( propertyId );
                currentRecord.setPrevProp( prevRecord.getId() );
                propRecords.add( currentRecord );
                // Now current is ready to start picking up blocks
            }
            currentRecord.addPropertyBlock( block );
        }
        /*
         * Add the property records in reverse order, which means largest
         * id first. That is to make sure we expand the property store file
         * only once.
         */
        for ( int i = propRecords.size() - 1; i >= 0; i-- )
        {
            propertyStore.updateRecord( propRecords.get( i ) );
        }
        /*
         *  0 will always exist, if the map was empty we wouldn't be here
         *  and even one property will create at least one record.
         */
        return propRecords.get( 0 ).getId();
    }
}
