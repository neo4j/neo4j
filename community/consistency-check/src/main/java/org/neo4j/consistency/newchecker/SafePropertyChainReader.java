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
package org.neo4j.consistency.newchecker;

import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.neo4j.consistency.newchecker.RecordLoading.NO_DYNAMIC_HANDLER;
import static org.neo4j.consistency.newchecker.RecordLoading.checkValidToken;
import static org.neo4j.consistency.newchecker.RecordLoading.lightClear;
import static org.neo4j.consistency.newchecker.RecordLoading.safeLoadDynamicRecordChain;
import static org.neo4j.io.IOUtils.closeAllUnchecked;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;

/**
 * A Property chain reader (and optionally checker) which can read, detect and abort broken property chains where a normal PropertyCursor
 * would have thrown exception on inconsistent chain.
 */
class SafePropertyChainReader implements AutoCloseable
{
    private final int stringStoreBlockSize;
    private final int arrayStoreBlockSize;
    private final PropertyStore propertyStore;
    private final RecordReader<PropertyRecord> propertyReader;
    private final RecordReader<DynamicRecord> stringReader;
    private final RecordReader<DynamicRecord> arrayReader;
    private final MutableLongSet seenRecords;
    private final MutableLongSet seenDynamicRecordIds;
    private final List<DynamicRecord> dynamicRecords;
    private final ConsistencyReport.Reporter reporter;
    private final CheckerContext context;
    private final NeoStores neoStores;

    SafePropertyChainReader( CheckerContext context )
    {
        this.context = context;
        this.neoStores = context.neoStores;
        this.reporter = context.reporter;
        this.stringStoreBlockSize = neoStores.getPropertyStore().getStringStore().getRecordDataSize();
        this.arrayStoreBlockSize = neoStores.getPropertyStore().getArrayStore().getRecordDataSize();
        this.propertyStore = neoStores.getPropertyStore();
        this.propertyReader = new RecordReader<>( neoStores.getPropertyStore() );
        this.stringReader = new RecordReader<>( neoStores.getPropertyStore().getStringStore() );
        this.arrayReader = new RecordReader<>( neoStores.getPropertyStore().getArrayStore() );
        this.seenRecords = new LongHashSet();
        this.seenDynamicRecordIds = new LongHashSet();
        this.dynamicRecords = new ArrayList<>();
    }

    <PRIMITIVE extends PrimitiveRecord> boolean read( MutableIntObjectMap<Value> intoValues, PRIMITIVE entity,
            Function<PRIMITIVE,ConsistencyReport.PrimitiveConsistencyReport> primitiveReporter )
    {
        lightClear( seenRecords );
        long propertyRecordId = entity.getNextProp();
        long previousRecordId = NULL_REFERENCE.longValue();
        boolean chainIsOk = true;
        while ( !NULL_REFERENCE.is( propertyRecordId ) && !context.isCancelled() )
        {
            if ( !seenRecords.add( propertyRecordId ) )
            {
                primitiveReporter.apply( entity ).propertyChainContainsCircularReference( propertyReader.record() );
                chainIsOk = false;
                break;
            }

            PropertyRecord propertyRecord = propertyReader.read( propertyRecordId );
            if ( !propertyRecord.inUse() )
            {
                primitiveReporter.apply( entity ).propertyNotInUse( propertyRecord );
                reporter.forProperty( context.recordLoader.property( previousRecordId ) ).nextNotInUse( propertyRecord );
                chainIsOk = false;
            }
            else
            {
                if ( propertyRecord.getPrevProp() != previousRecordId )
                {
                    if ( NULL_REFERENCE.is( previousRecordId ) )
                    {
                        primitiveReporter.apply( entity ).propertyNotFirstInChain( propertyRecord );
                    }
                    else
                    {
                        reporter.forProperty( context.recordLoader.property( previousRecordId ) ).nextDoesNotReferenceBack( propertyRecord );
                        // prevDoesNotReferenceBack is not reported, unnecessary double report (same inconsistency from different directions)
                    }
                    chainIsOk = false;
                }

                for ( PropertyBlock block : propertyRecord )
                {
                    int propertyKeyId = block.getKeyIndexId();
                    if ( !checkValidToken( propertyRecord, propertyKeyId, context.tokenHolders.propertyKeyTokens(), neoStores.getPropertyKeyTokenStore(),
                            ( property, token ) -> reporter.forProperty( property ).invalidPropertyKey( block ),
                            ( property, token ) -> reporter.forProperty( property ).keyNotInUse( block, token ) ) )
                    {
                        chainIsOk = false;
                    }
                    PropertyType type = block.forceGetType();
                    Value value = Values.NO_VALUE;
                    if ( type == null )
                    {
                        reporter.forProperty( propertyRecord ).invalidPropertyType( block );
                    }
                    else
                    {
                        try
                        {
                            switch ( type )
                            {
                            case STRING:
                                dynamicRecords.clear();
                                if ( safeLoadDynamicRecordChain( record -> dynamicRecords.add( record.copy() ), stringReader, seenDynamicRecordIds,
                                        block.getSingleValueLong(), stringStoreBlockSize, NO_DYNAMIC_HANDLER,
                                        ( id, record ) -> reporter.forProperty( propertyRecord ).stringNotInUse( block, record ),
                                        ( id, record ) -> reporter.forDynamicBlock( RecordType.STRING_PROPERTY, stringReader.record() ).nextNotInUse( record ),
                                        ( id, record ) -> reporter.forProperty( propertyRecord ).stringEmpty( block, record ),
                                        record -> reporter.forDynamicBlock( RecordType.STRING_PROPERTY, record ).recordNotFullReferencesNext(),
                                        record -> reporter.forDynamicBlock( RecordType.STRING_PROPERTY, record ).invalidLength() ) )
                                {
                                    value = Values.stringValue( propertyStore.getStringFor( dynamicRecords ) );
                                }
                                break;
                            case ARRAY:
                                dynamicRecords.clear();
                                if ( safeLoadDynamicRecordChain( record -> dynamicRecords.add( record.copy() ), arrayReader, seenDynamicRecordIds,
                                        block.getSingleValueLong(), arrayStoreBlockSize, NO_DYNAMIC_HANDLER,
                                        ( id, record ) -> reporter.forProperty( propertyRecord ).arrayNotInUse( block, record ),
                                        ( id, record ) -> reporter.forDynamicBlock( RecordType.ARRAY_PROPERTY, arrayReader.record() ).nextNotInUse( record ),
                                        ( id, record ) -> reporter.forProperty( propertyRecord ).arrayEmpty( block, record ),
                                        record -> reporter.forDynamicBlock( RecordType.ARRAY_PROPERTY, record ).recordNotFullReferencesNext(),
                                        record -> reporter.forDynamicBlock( RecordType.ARRAY_PROPERTY, record ).invalidLength() ) )
                                {
                                    value = propertyStore.getArrayFor( dynamicRecords );
                                }
                                break;
                            default:
                                value = type.value( block, null );
                                break;
                            }
                        }
                        catch ( Exception e )
                        {
                            reporter.forProperty( propertyRecord ).invalidPropertyValue( propertyRecord.getId(), block.getKeyIndexId() );
                        }
                    }
                    if ( value == Values.NO_VALUE )
                    {
                        chainIsOk = false;
                    }
                    else if ( propertyKeyId >= 0 && intoValues.put( propertyKeyId, value ) != null )
                    {
                        primitiveReporter.apply( entity ).propertyKeyNotUniqueInChain();
                        chainIsOk = false;
                    }
                }
            }
            previousRecordId = propertyRecordId;
            propertyRecordId = propertyRecord.getNextProp();
        }
        return chainIsOk;
    }

    @Override
    public void close()
    {
        closeAllUnchecked( propertyReader, stringReader, arrayReader );
    }
}
