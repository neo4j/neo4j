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
package org.neo4j.kernel.impl.nioneo.xa;

import java.util.Iterator;

import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecordAllocator;
import org.neo4j.kernel.impl.nioneo.store.IdSequence;
import org.neo4j.kernel.impl.nioneo.store.PrimitiveRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.xa.RecordAccess.RecordProxy;

public class PropertyCreator
{
    private final DynamicRecordAllocator stringRecordAllocator;
    private final DynamicRecordAllocator arrayRecordAllocator;
    private final IdSequence propertyRecordIdGenerator;
    private final PropertyTraverser traverser;

    public PropertyCreator( PropertyStore propertyStore, PropertyTraverser traverser )
    {
        this( propertyStore.getStringStore(), propertyStore.getArrayStore(), propertyStore, traverser );
    }

    public PropertyCreator( DynamicRecordAllocator stringRecordAllocator, DynamicRecordAllocator arrayRecordAllocator,
            IdSequence propertyRecordIdGenerator, PropertyTraverser traverser )
    {
        this.stringRecordAllocator = stringRecordAllocator;
        this.arrayRecordAllocator = arrayRecordAllocator;
        this.propertyRecordIdGenerator = propertyRecordIdGenerator;
        this.traverser = traverser;
    }

    public <P extends PrimitiveRecord> void primitiveChangeProperty(
            RecordProxy<Long, P, Void> primitiveRecordChange, int propertyKey, Object value,
            RecordAccess<Long, PropertyRecord, PrimitiveRecord> propertyRecords )
    {
        P primitive = primitiveRecordChange.forReadingLinkage();
        assert traverser.assertPropertyChain( primitive, propertyRecords );
        long propertyId = // propertyData.getId();
                traverser.findPropertyRecordContaining( primitive, propertyKey, propertyRecords, true );
        PropertyRecord propertyRecord = propertyRecords.getOrLoad( propertyId, primitive ).forChangingData();
        if ( !propertyRecord.inUse() )
        {
            throw new IllegalStateException( "Unable to change property["
                                             + propertyId
                                             + "] since it has been deleted." );
        }
        PropertyBlock block = propertyRecord.getPropertyBlock( propertyKey );
        if ( block == null )
        {
            throw new IllegalStateException( "Property with index["
                                             + propertyKey
                                             + "] is not present in property["
                                             + propertyId + "]" );
        }
        propertyRecord.setChanged( primitive );
        for ( DynamicRecord record : block.getValueRecords() )
        {
            assert record.inUse();
            record.setInUse( false, block.getType().intValue() );
            propertyRecord.addDeletedRecord( record );
        }
        encodeValue( block, propertyKey, value );
        if ( propertyRecord.size() > PropertyType.getPayloadSize() )
        {
            propertyRecord.removePropertyBlock( propertyKey );
            /*
             * The record should never, ever be above max size. Less obviously, it should
             * never remain empty. If removing a property because it won't fit when changing
             * it leaves the record empty it means that this block was the last one which
             * means that it doesn't fit in an empty record. Where i come from, we call this
             * weird.
             *
             assert propertyRecord.size() <= PropertyType.getPayloadSize() : propertyRecord;
             assert propertyRecord.size() > 0 : propertyRecord;
             */
            addPropertyBlockToPrimitive( block, primitiveRecordChange, propertyRecords );
        }
        assert traverser.assertPropertyChain( primitive, propertyRecords );
    }

    public PropertyBlock encodePropertyValue( int propertyKey, Object value )
    {
        return encodeValue( new PropertyBlock(), propertyKey, value );
    }

    public PropertyBlock encodeValue( PropertyBlock block, int propertyKey, Object value )
    {
        PropertyStore.encodeValue( block, propertyKey, value, stringRecordAllocator, arrayRecordAllocator );
        return block;
    }

    public <P extends PrimitiveRecord> void primitiveAddProperty(
            RecordProxy<Long, P, Void> primitive, int propertyKey, Object value,
            RecordAccess<Long, PropertyRecord, PrimitiveRecord> propertyRecords )
    {
        P record = primitive.forReadingLinkage();
        assert traverser.assertPropertyChain( record, propertyRecords );
        PropertyBlock block = new PropertyBlock();
        encodeValue( block, propertyKey, value );
        addPropertyBlockToPrimitive( block, primitive, propertyRecords );
        assert traverser.assertPropertyChain( record, propertyRecords );
    }

    private <P extends PrimitiveRecord> void addPropertyBlockToPrimitive(
            PropertyBlock block, RecordProxy<Long, P, Void> primitiveRecordChange,
            RecordAccess<Long, PropertyRecord, PrimitiveRecord> propertyRecords )
    {
        P primitive = primitiveRecordChange.forReadingLinkage();
        assert traverser.assertPropertyChain( primitive, propertyRecords );
        int newBlockSizeInBytes = block.getSize();
        /*
         * Here we could either iterate over the whole chain or just go for the first record
         * which is the most likely to be the less full one. Currently we opt for the second
         * to perform better.
         */
        PropertyRecord host = null;
        long firstProp = primitive.getNextProp();
        if ( firstProp != Record.NO_NEXT_PROPERTY.intValue() )
        {
            // We do not store in map - might not have enough space
            RecordProxy<Long, PropertyRecord, PrimitiveRecord> change =
                    propertyRecords.getOrLoad( firstProp, primitive );
            PropertyRecord propRecord = change.forReadingLinkage();
            assert propRecord.getPrevProp() == Record.NO_PREVIOUS_PROPERTY.intValue() : propRecord
                                                                                        + " for "
                                                                                        + primitive;
            assert propRecord.inUse() : propRecord;
            int propSize = propRecord.size();
            assert propSize > 0 : propRecord;
            if ( propSize + newBlockSizeInBytes <= PropertyType.getPayloadSize() )
            {
                propRecord = change.forChangingData();
                host = propRecord;
                host.addPropertyBlock( block );
                host.setChanged( primitive );
            }
        }
        if ( host == null )
        {
            // First record in chain didn't fit, make new one
            host = propertyRecords.create( propertyRecordIdGenerator.nextId(), primitive ).forChangingData();
            if ( primitive.getNextProp() != Record.NO_NEXT_PROPERTY.intValue() )
            {
                PropertyRecord prevProp = propertyRecords.getOrLoad( primitive.getNextProp(),
                        primitive ).forChangingLinkage();
                assert prevProp.getPrevProp() == Record.NO_PREVIOUS_PROPERTY.intValue();
                prevProp.setPrevProp( host.getId() );
                host.setNextProp( prevProp.getId() );
                prevProp.setChanged( primitive );
            }
            primitiveRecordChange.forChangingLinkage().setNextProp( host.getId() );
            host.addPropertyBlock( block );
            host.setInUse( true );
        }
        // Ok, here host does for the job. Use it
        assert traverser.assertPropertyChain( primitive, propertyRecords );
    }

    public long createPropertyChain( PrimitiveRecord owner, Iterator<PropertyBlock> properties,
            RecordAccess<Long, PropertyRecord, PrimitiveRecord> propertyRecords )
    {
        if ( properties == null || !properties.hasNext() )
        {
            return Record.NO_NEXT_PROPERTY.intValue();
        }
        PropertyRecord currentRecord = propertyRecords.create( propertyRecordIdGenerator.nextId(), owner )
                .forChangingData();
        currentRecord.setInUse( true );
        currentRecord.setCreated();
        PropertyRecord firstRecord = currentRecord;
        while ( properties.hasNext() )
        {
            PropertyBlock block = properties.next();
            if ( currentRecord.size() + block.getSize() > PropertyType.getPayloadSize() )
            {
                // Here it means the current block is done for
                PropertyRecord prevRecord = currentRecord;
                // Create new record
                long propertyId = propertyRecordIdGenerator.nextId();
                currentRecord = propertyRecords.create( propertyId, owner ).forChangingData();
                currentRecord.setInUse( true );
                currentRecord.setCreated();
                // Set up links
                prevRecord.setNextProp( propertyId );
                currentRecord.setPrevProp( prevRecord.getId() );
                // Now current is ready to start picking up blocks
            }
            currentRecord.addPropertyBlock( block );
        }
        return firstRecord.getId();
    }

    public <P extends PrimitiveRecord> void setPrimitiveProperty(
            RecordProxy<Long, P, Void> primitiveProxy, int key, Object value,
            RecordAccess<Long, PropertyRecord, PrimitiveRecord> propertyRecords )
    {
        PrimitiveRecord primitive = primitiveProxy.forReadingLinkage();
        long nextProp = primitive.getNextProp();
        PropertyBlock block = new PropertyBlock();
        encodeValue( block, key, value );
        int size = block.getSize();

        /*
         * current is the current record traversed
         * thatFits is the earliest record that can host the block
         * thatHas is the record that already has a block for this index
         */
        RecordProxy<Long, PropertyRecord, PrimitiveRecord> current, thatFits = null, thatHas = null;

        /*
         * We keep going while there are records or until we both found the
         * property if it exists and the place to put it, if exists.
         */
        while ( !(nextProp == Record.NO_NEXT_PROPERTY.intValue() || (thatHas != null && thatFits != null)) )
        {
            current = propertyRecords.getOrLoad( nextProp, primitive );
            /*
             * current.getPropertyBlock() is cheap but not free. If we already
             * have found thatHas, then we can skip this lookup.
             */
            if ( thatHas == null && current.forReadingLinkage().getPropertyBlock( key ) != null )
            {
                thatHas = current;

                PropertyBlock removed = thatHas.forChangingData().removePropertyBlock( key );
                for ( DynamicRecord dynRec : removed.getValueRecords() )
                {
                    dynRec.setInUse( false );
                    thatHas.forChangingData().addDeletedRecord( dynRec );
                }
            }
            /*
             * We check the size after we remove - potentially we can put in the same record.
             *
             * current.size() is cheap but not free. If we already found somewhere
             * where it fits, no need to look again.
             */
            if ( thatFits == null && (PropertyType.getPayloadSize() - current.forReadingLinkage().size() >= size) )
            {
                thatFits = current;
            }
            nextProp = current.forReadingLinkage().getNextProp();
        }

        /*
         * thatHas is of no importance here. We know that the block is definitely not there.
         * However, we can be sure that if the property existed, thatHas is not null and does
         * not contain the block.
         *
         * thatFits is interesting. If null, we need to create a new record and link, otherwise
         * just add the block there.
         */
        if ( thatFits == null )
        {
            thatFits = propertyRecords.create( propertyRecordIdGenerator.nextId(), primitive );
            PropertyRecord thatFitsRecord = thatFits.forChangingData();
            thatFitsRecord.setInUse( true );

            if ( primitive.getNextProp() != Record.NO_NEXT_PROPERTY.intValue() )
            {
                PropertyRecord first = propertyRecords.getOrLoad( primitive.getNextProp(),
                        primitive ).forChangingLinkage();
                thatFitsRecord.setNextProp( first.getId() );
                first.setPrevProp( thatFitsRecord.getId() );
            }
            primitiveProxy.forChangingLinkage().setNextProp( thatFitsRecord.getId() );
        }

        thatFits.forChangingData().addPropertyBlock( block );
    }
}
