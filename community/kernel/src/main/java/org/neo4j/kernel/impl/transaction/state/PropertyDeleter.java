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

import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.transaction.state.RecordAccess.RecordProxy;

public class PropertyDeleter
{
    private final PropertyTraverser traverser;

    public PropertyDeleter( PropertyTraverser traverser )
    {
        this.traverser = traverser;
    }

    public void deletePropertyChain( PrimitiveRecord primitive,
            RecordAccess<PropertyRecord, PrimitiveRecord> propertyRecords )
    {
        long nextProp = primitive.getNextProp();
        while ( nextProp != Record.NO_NEXT_PROPERTY.intValue() )
        {
            RecordProxy<PropertyRecord, PrimitiveRecord> propertyChange =
                    propertyRecords.getOrLoad( nextProp, primitive );

            // TODO forChanging/forReading piggy-backing
            PropertyRecord propRecord = propertyChange.forChangingData();
            deletePropertyRecordIncludingValueRecords( propRecord );
            nextProp = propRecord.getNextProp();
            propRecord.setChanged( primitive );
        }
        primitive.setNextProp( Record.NO_NEXT_PROPERTY.intValue() );
    }

    public static void deletePropertyRecordIncludingValueRecords( PropertyRecord record )
    {
        for ( PropertyBlock block : record )
        {
            for ( DynamicRecord valueRecord : block.getValueRecords() )
            {
                assert valueRecord.inUse();
                valueRecord.setInUse( false );
                record.addDeletedRecord( valueRecord );
            }
        }
        record.clearPropertyBlocks();
        record.setInUse( false );
    }

    /**
     * Removes property with given {@code propertyKey} from property chain owner by the primitive found in
     * {@code primitiveProxy} if it exists.
     *
     * @param primitiveProxy access to the primitive record pointing to the start of the property chain.
     * @param propertyKey the property key token id to look for and remove.
     * @param propertyRecords access to records.
     * @return {@code true} if the property was found and removed, otherwise {@code false}.
     */
    public <P extends PrimitiveRecord> boolean removePropertyIfExists( RecordProxy<P,Void> primitiveProxy,
            int propertyKey, RecordAccess<PropertyRecord,PrimitiveRecord> propertyRecords )
    {
        PrimitiveRecord primitive = primitiveProxy.forReadingData();
        long propertyId = // propertyData.getId();
                traverser.findPropertyRecordContaining( primitive, propertyKey, propertyRecords, false );
        if ( !Record.NO_NEXT_PROPERTY.is( propertyId ) )
        {
            removeProperty( primitiveProxy, propertyKey, propertyRecords, primitive, propertyId );
            return true;
        }
        return false;
    }

    /**
     * Removes property with given {@code propertyKey} from property chain owner by the primitive found in
     * {@code primitiveProxy}.
     *
     * @param primitiveProxy access to the primitive record pointing to the start of the property chain.
     * @param propertyKey the property key token id to look for and remove.
     * @param propertyRecords access to records.
     * @throws IllegalStateException if property key was not found in the property chain.
     */
    public <P extends PrimitiveRecord> void removeProperty( RecordProxy<P,Void> primitiveProxy, int propertyKey,
            RecordAccess<PropertyRecord,PrimitiveRecord> propertyRecords )
    {
        PrimitiveRecord primitive = primitiveProxy.forReadingData();
        long propertyId = // propertyData.getId();
                traverser.findPropertyRecordContaining( primitive, propertyKey, propertyRecords, true );
        removeProperty( primitiveProxy, propertyKey, propertyRecords, primitive, propertyId );
    }

    private <P extends PrimitiveRecord> void removeProperty( RecordProxy<P,Void> primitiveProxy, int propertyKey,
            RecordAccess<PropertyRecord,PrimitiveRecord> propertyRecords, PrimitiveRecord primitive,
            long propertyId )
    {
        RecordProxy<PropertyRecord, PrimitiveRecord> recordChange =
                propertyRecords.getOrLoad( propertyId, primitive );
        PropertyRecord propRecord = recordChange.forChangingData();
        if ( !propRecord.inUse() )
        {
            throw new IllegalStateException( "Unable to delete property[" +
                    propertyId + "] since it is already deleted." );
        }

        PropertyBlock block = propRecord.removePropertyBlock( propertyKey );
        if ( block == null )
        {
            throw new IllegalStateException( "Property with index["
                                             + propertyKey
                                             + "] is not present in property["
                                             + propertyId + "]" );
        }

        for ( DynamicRecord valueRecord : block.getValueRecords() )
        {
            assert valueRecord.inUse();
            valueRecord.setInUse( false, block.getType().intValue() );
            propRecord.addDeletedRecord( valueRecord );
        }
        if ( propRecord.size() > 0 )
        {
            /*
             * There are remaining blocks in the record. We do not unlink yet.
             */
            propRecord.setChanged( primitive );
            assert traverser.assertPropertyChain( primitive, propertyRecords );
        }
        else
        {
            unlinkPropertyRecord( propRecord, propertyRecords, primitiveProxy );
        }
    }

    private <P extends PrimitiveRecord> void unlinkPropertyRecord( PropertyRecord propRecord,
            RecordAccess<PropertyRecord,PrimitiveRecord> propertyRecords,
            RecordProxy<P, Void> primitiveRecordChange )
    {
        P primitive = primitiveRecordChange.forReadingLinkage();
        assert traverser.assertPropertyChain( primitive, propertyRecords );
        assert propRecord.size() == 0;
        long prevProp = propRecord.getPrevProp();
        long nextProp = propRecord.getNextProp();
        if ( primitive.getNextProp() == propRecord.getId() )
        {
            assert propRecord.getPrevProp() == Record.NO_PREVIOUS_PROPERTY.intValue() : propRecord
                    + " for "
                    + primitive;
            primitiveRecordChange.forChangingLinkage().setNextProp( nextProp );
        }
        if ( prevProp != Record.NO_PREVIOUS_PROPERTY.intValue() )
        {
            PropertyRecord prevPropRecord = propertyRecords.getOrLoad( prevProp, primitive ).forChangingLinkage();
            assert prevPropRecord.inUse() : prevPropRecord + "->" + propRecord
            + " for " + primitive;
            prevPropRecord.setNextProp( nextProp );
            prevPropRecord.setChanged( primitive );
        }
        if ( nextProp != Record.NO_NEXT_PROPERTY.intValue() )
        {
            PropertyRecord nextPropRecord = propertyRecords.getOrLoad( nextProp, primitive ).forChangingLinkage();
            assert nextPropRecord.inUse() : propRecord + "->" + nextPropRecord
            + " for " + primitive;
            nextPropRecord.setPrevProp( prevProp );
            nextPropRecord.setChanged( primitive );
        }
        propRecord.setInUse( false );
        /*
         *  The following two are not needed - the above line does all the work (PropertyStore
         *  does not write out the prev/next for !inUse records). It is nice to set this
         *  however to check for consistency when assertPropertyChain().
         */
        propRecord.setPrevProp( Record.NO_PREVIOUS_PROPERTY.intValue() );
        propRecord.setNextProp( Record.NO_NEXT_PROPERTY.intValue() );
        propRecord.setChanged( primitive );
        assert traverser.assertPropertyChain( primitive, propertyRecords );
    }
}
