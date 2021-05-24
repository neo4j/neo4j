/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.internal.recordstorage;

import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.eclipse.collections.impl.factory.primitive.LongSets;

import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.internal.recordstorage.RecordAccess.RecordProxy;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.values.storable.Value;

import static java.lang.StrictMath.toIntExact;
import static java.lang.String.format;
import static org.neo4j.internal.recordstorage.InconsistentDataReadException.CYCLE_DETECTION_THRESHOLD;

public class PropertyDeleter
{
    private final PropertyTraverser traverser;
    private final NeoStores neoStores;
    private final TokenNameLookup tokenNameLookup;
    private final LogProvider logProvider;
    private final Config config;
    private final PageCursorTracer cursorTracer;
    private final MemoryTracker memoryTracker;

    public PropertyDeleter( PropertyTraverser traverser, NeoStores neoStores, TokenNameLookup tokenNameLookup, LogProvider logProvider, Config config,
            PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
    {
        this.traverser = traverser;
        this.neoStores = neoStores;
        this.tokenNameLookup = tokenNameLookup;
        this.logProvider = logProvider;
        this.config = config;
        this.cursorTracer = cursorTracer;
        this.memoryTracker = memoryTracker;
    }

    public void deletePropertyChain( PrimitiveRecord primitive,
            RecordAccess<PropertyRecord, PrimitiveRecord> propertyRecords )
    {
        long nextProp = primitive.getNextProp();
        MutableLongSet seenPropertyIds = null;
        int count = 0;
        try
        {
            while ( nextProp != Record.NO_NEXT_PROPERTY.longValue() )
            {
                RecordProxy<PropertyRecord,PrimitiveRecord> propertyChange = propertyRecords.getOrLoad( nextProp, primitive, cursorTracer );
                PropertyRecord propRecord = propertyChange.forChangingData();
                deletePropertyRecordIncludingValueRecords( propRecord );
                if ( ++count >= CYCLE_DETECTION_THRESHOLD )
                {
                    if ( seenPropertyIds == null )
                    {
                        seenPropertyIds = LongSets.mutable.empty();
                    }
                    if ( !seenPropertyIds.add( nextProp ) )
                    {
                        throw new InconsistentDataReadException( "Cycle detected in property chain for %s", primitive );
                    }
                }
                nextProp = propRecord.getNextProp();
                propRecord.setChanged( primitive );
            }
        }
        catch ( InvalidRecordException e )
        {
            // This property chain, or a dynamic value record chain contains a record which is not in use, so it's somewhat broken.
            // Abort reading the chain, but don't fail the deletion of this property record chain.
            logInconsistentPropertyChain( primitive, "unused record", e );
        }
        catch ( InconsistentDataReadException e )
        {
            // This property chain, or a dynamic value record chain contains a cycle.
            // Abort reading the chain, but don't fail the deletion of this property record chain.
            logInconsistentPropertyChain( primitive, "cycle", e );
        }
        primitive.setNextProp( Record.NO_NEXT_PROPERTY.intValue() );
    }

    private void logInconsistentPropertyChain( PrimitiveRecord primitive, String causeMessage, Throwable cause )
    {
        if ( !config.get( GraphDatabaseInternalSettings.log_inconsistent_data_deletion ) )
        {
            return;
        }

        StringBuilder message = new StringBuilder( format( "Deleted inconsistent property chain with %s for %s", causeMessage, primitive ) );
        try ( RecordPropertyCursor propertyCursor = new RecordPropertyCursor( neoStores.getPropertyStore(), cursorTracer, memoryTracker ) )
        {
            if ( primitive instanceof NodeRecord )
            {
                NodeRecord node = (NodeRecord) primitive;
                message.append( " with labels: " );
                long[] labelIds = NodeLabelsField.parseLabelsField( node ).get( neoStores.getNodeStore(), cursorTracer );
                message.append(
                        LongStream.of( labelIds ).mapToObj( labelId -> tokenNameLookup.labelGetName( toIntExact( labelId ) ) ).collect( Collectors.toList() ) );
                propertyCursor.initNodeProperties( node.getNextProp(), node.getId() );
            }
            else if ( primitive instanceof RelationshipRecord )
            {
                RelationshipRecord relationship = (RelationshipRecord) primitive;
                message.append( format( " with relationship type: %s", tokenNameLookup.relationshipTypeGetName( relationship.getType() ) ) );
                propertyCursor.initRelationshipProperties( relationship.getNextProp(), relationship.getId() );
            }

            // Use the cursor to read property values, because it's more flexible in reading data
            MutableIntSet seenKeyIds = IntSets.mutable.empty();
            while ( propertyCursor.next() )
            {
                int keyId = propertyCursor.propertyKey();
                if ( !seenKeyIds.add( keyId ) )
                {
                    continue;
                }

                String key = tokenNameLookup.propertyKeyGetName( keyId );
                Value value;
                try
                {
                    value = propertyCursor.propertyValue();
                }
                catch ( Exception e )
                {
                    value = null;
                }
                String valueToString = value != null ? value.toString() : "<value could not be read>";
                message.append( format( "%n  %s = %s", key, valueToString ) );
            }
        }
        catch ( InconsistentDataReadException e )
        {
            // Expected to occur on chain cycles, that's what we're here for
        }
        logProvider.getLog( InconsistentDataDeletion.class ).error( message.toString(), cause );
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
        RecordProxy<PropertyRecord, PrimitiveRecord> recordChange = propertyRecords.getOrLoad( propertyId, primitive, cursorTracer );
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
            PropertyRecord prevPropRecord = propertyRecords.getOrLoad( prevProp, primitive, cursorTracer ).forChangingLinkage();
            assert prevPropRecord.inUse() : prevPropRecord + "->" + propRecord
            + " for " + primitive;
            prevPropRecord.setNextProp( nextProp );
            prevPropRecord.setChanged( primitive );
        }
        if ( nextProp != Record.NO_NEXT_PROPERTY.intValue() )
        {
            PropertyRecord nextPropRecord = propertyRecords.getOrLoad( nextProp, primitive, cursorTracer ).forChangingLinkage();
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
