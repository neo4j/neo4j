/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.consistency.checker;

import org.eclipse.collections.api.collection.primitive.MutableIntCollection;
import org.eclipse.collections.api.collection.primitive.MutableLongCollection;
import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.eclipse.collections.api.map.primitive.MutablePrimitiveObjectMap;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.function.ThrowingIntFunction;
import org.neo4j.internal.schema.PropertySchemaType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.DynamicNodeLabels;
import org.neo4j.kernel.impl.store.DynamicStringStore;
import org.neo4j.kernel.impl.store.InlineNodeLabels;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.TokenRecord;
import org.neo4j.token.api.NamedToken;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.token.api.TokenNotFoundException;
import org.neo4j.values.storable.Value;

import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;
import static org.neo4j.values.storable.Values.NO_VALUE;

/**
 * Loads records. This is only meant to be used when actually finding inconsistencies.
 */
class RecordLoading
{
    static final BiConsumer<Long,DynamicRecord> NO_DYNAMIC_HANDLER = ( id, r ) -> {};
    private final NeoStores neoStores;

    RecordLoading( NeoStores neoStores )
    {
        this.neoStores = neoStores;
    }

    static long[] safeGetNodeLabels( CheckerContext context, long nodeId, long labelField, RecordReader<DynamicRecord> labelReader,
            PageCursorTracer cursorTracer )
    {
        if ( !NodeLabelsField.fieldPointsToDynamicRecordOfLabels( labelField ) )
        {
            return InlineNodeLabels.parseInlined( labelField );
        }

        // The idea here is that we don't pass in a lot of cursors and stuff because dynamic labels are so rare?
        List<DynamicRecord> records = new ArrayList<>();
        MutableLongSet seenRecordIds = new LongHashSet();
        ConsistencyReport.Reporter reporter = context.reporter;
        RecordLoading recordLoader = context.recordLoader;
        int nodeLabelBlockSize = context.neoStores.getNodeStore().getDynamicLabelStore().getRecordDataSize();
        if ( safeLoadDynamicRecordChain( record -> records.add( record.copy() ), labelReader, seenRecordIds,
                NodeLabelsField.firstDynamicLabelRecordId( labelField ), nodeLabelBlockSize,
                ( id, labelRecord ) -> reporter.forNode( recordLoader.node( nodeId, cursorTracer ) ).dynamicRecordChainCycle( labelRecord ),
                ( id, labelRecord ) -> reporter.forNode( recordLoader.node( nodeId, cursorTracer ) ).dynamicLabelRecordNotInUse( labelRecord ),
                ( id, labelRecord ) -> reporter.forNode( recordLoader.node( nodeId, cursorTracer ) ).dynamicLabelRecordNotInUse( labelRecord ),
                ( id, labelRecord ) -> reporter.forDynamicBlock( RecordType.NODE_DYNAMIC_LABEL, labelRecord ).emptyBlock(),
                labelRecord -> reporter.forDynamicBlock( RecordType.NODE_DYNAMIC_LABEL, labelRecord ).recordNotFullReferencesNext(),
                labelRecord -> reporter.forDynamicBlock( RecordType.NODE_DYNAMIC_LABEL, labelRecord ).invalidLength() ) )
        {
            return DynamicNodeLabels.getDynamicLabelsArray( records, labelReader.store(), cursorTracer );
        }
        return null;
    }

    private static Value[] matchAllProperties( IntObjectMap<Value> values, int[] propertyKeyIds )
    {
        Value[] array = new Value[propertyKeyIds.length];
        for ( int i = 0; i < propertyKeyIds.length; i++ )
        {
            int propertyKeyId = propertyKeyIds[i];
            Value value = values.get( propertyKeyId );
            if ( value == null )
            {
                return null;
            }
            array[i] = value;
        }
        return array;
    }

    private static Value[] matchAnyProperty( IntObjectMap<Value> values, int[] propertyKeyIds )
    {
        Value[] array = new Value[propertyKeyIds.length];
        boolean anyFound = false;
        for ( int i = 0; i < propertyKeyIds.length; i++ )
        {
            Value value = values.get( propertyKeyIds[i] );
            if ( value != null )
            {
                anyFound = true;
            }
            else
            {
                value = NO_VALUE;
            }
            array[i] = value;
        }
        return anyFound ? array : null;
    }

    static Value[] entityIntersectionWithSchema( long[] entityTokens, IntObjectMap<Value> values, SchemaDescriptor schema )
    {
        Value[] valueArray = null;
        if ( schema.isAffected( entityTokens ) )
        {
            boolean requireAllTokens = schema.propertySchemaType() == PropertySchemaType.COMPLETE_ALL_TOKENS;
            valueArray = requireAllTokens ? matchAllProperties( values, schema.getPropertyIds() ) : matchAnyProperty( values, schema.getPropertyIds() );
        }
        // else this entity should not be in this index. This check is done in a sequential manner elsewhere
        return valueArray;
    }

    NodeRecord node( long id, PageCursorTracer cursorTracer )
    {
        return loadRecord( neoStores.getNodeStore(), id, cursorTracer );
    }

    PropertyRecord property( long id, PageCursorTracer cursorTracer )
    {
        return loadRecord( neoStores.getPropertyStore(), id, cursorTracer );
    }

    RelationshipRecord relationship( long id, PageCursorTracer cursorTracer )
    {
        return loadRecord( neoStores.getRelationshipStore(), id, cursorTracer );
    }

    RelationshipRecord relationship( RelationshipRecord into, long id, PageCursorTracer cursorTracer )
    {
        return loadRecord( neoStores.getRelationshipStore(), into, id, cursorTracer );
    }

    RelationshipGroupRecord relationshipGroup( long id, PageCursorTracer cursorTracer )
    {
        return loadRecord( neoStores.getRelationshipGroupStore(), id, cursorTracer );
    }

    <RECORD extends AbstractBaseRecord> RECORD loadRecord( RecordStore<RECORD> store, long id, PageCursorTracer cursorTracer )
    {
        return loadRecord( store, store.newRecord(), id, cursorTracer );
    }

    <RECORD extends AbstractBaseRecord> RECORD loadRecord( RecordStore<RECORD> store, RECORD record, long id, PageCursorTracer cursorTracer )
    {
        return store.getRecord( id, record, RecordLoad.FORCE, cursorTracer );
    }

    static <RECORD extends TokenRecord> List<NamedToken> safeLoadTokens( TokenStore<RECORD> tokenStore, PageCursorTracer cursorTracer )
    {
        long highId = tokenStore.getHighId();
        List<NamedToken> tokens = new ArrayList<>();
        DynamicStringStore nameStore = tokenStore.getNameStore();
        List<DynamicRecord> nameRecords = new ArrayList<>();
        MutableLongSet seenRecordIds = new LongHashSet();
        int nameBlockSize = nameStore.getRecordDataSize();
        try ( RecordReader<RECORD> tokenReader = new RecordReader<>( tokenStore, cursorTracer );
              RecordReader<DynamicRecord> nameReader = new RecordReader<>( nameStore, cursorTracer ) )
        {
            for ( long id = 0; id < highId; id++ )
            {
                RECORD record = tokenReader.read( id );
                nameRecords.clear();
                if ( record.inUse() )
                {
                    String name;
                    if ( !NULL_REFERENCE.is( record.getNameId() ) && safeLoadDynamicRecordChain( r -> nameRecords.add( r.copy() ),
                            nameReader, seenRecordIds, record.getNameId(), nameBlockSize ) )
                    {
                        record.addNameRecords( nameRecords );
                        name = tokenStore.getStringFor( record, cursorTracer );
                    }
                    else
                    {
                        name = format( "<name not loaded due to token(%d) referencing unused name record>", id );
                    }
                    tokens.add( new NamedToken( name, toIntExact( id ), record.isInternal() ) );
                }
            }
        }
        return tokens;
    }

    static boolean safeLoadDynamicRecordChain( Consumer<DynamicRecord> target, RecordReader<DynamicRecord> reader,
            MutableLongSet seenRecordIds, long recordId, int blockSize )
    {
        return safeLoadDynamicRecordChain( target, reader, seenRecordIds, recordId, blockSize,
                NO_DYNAMIC_HANDLER, NO_DYNAMIC_HANDLER, NO_DYNAMIC_HANDLER, NO_DYNAMIC_HANDLER, r -> {}, r -> {} );
    }

    static boolean safeLoadDynamicRecordChain( Consumer<DynamicRecord> target, RecordReader<DynamicRecord> reader,
            MutableLongSet seenRecordIds, long recordId, int blockSize,
            BiConsumer<Long,DynamicRecord> circularReferenceReport,
            BiConsumer<Long,DynamicRecord> unusedChainReport,
            BiConsumer<Long,DynamicRecord> brokenChainReport,
            BiConsumer<Long,DynamicRecord> emptyRecordReport,
            Consumer<DynamicRecord> notFullReferencesNextReport,
            Consumer<DynamicRecord> invalidLengthReport )
    {
        long firstRecordId = recordId;
        lightClear( seenRecordIds );
        long prevRecordId = NULL_REFERENCE.longValue();
        boolean chainIsOk = true;
        while ( !NULL_REFERENCE.is( recordId ) )
        {
            if ( !seenRecordIds.add( recordId ) )
            {
                // Circular reference
                circularReferenceReport.accept( firstRecordId, reader.record() );
                return false;
            }
            DynamicRecord record = reader.read( recordId );
            if ( !record.inUse() )
            {
                // Broken chain somehow
                BiConsumer<Long,DynamicRecord> reporter = recordId == firstRecordId ? unusedChainReport : brokenChainReport;
                reporter.accept( prevRecordId, record );
                return false;
            }
            if ( record.getLength() == 0 )
            {
                // Empty record
                emptyRecordReport.accept( firstRecordId, record );
                chainIsOk = false;
            }
            if ( record.getLength() < blockSize && !NULL_REFERENCE.is( record.getNextBlock() ) )
            {
                notFullReferencesNextReport.accept( record );
                chainIsOk = false;
            }
            if ( record.getLength() > blockSize )
            {
                invalidLengthReport.accept( record );
                chainIsOk = false;
            }
            target.accept( record );
            prevRecordId = recordId;
            recordId = record.getNextBlock();
        }
        return chainIsOk;
    }

    static <RECORD extends AbstractBaseRecord,TOKEN extends TokenRecord> boolean checkValidInternalToken(
            RECORD entity, int token, TokenHolder tokens, TokenStore<TOKEN> tokenStore, BiConsumer<RECORD,Integer> illegalTokenReport,
            BiConsumer<RECORD,TOKEN> unusedReporter, PageCursorTracer cursorTracer )
    {
        return checkValidToken( entity, token, tokens, tokenStore, illegalTokenReport, unusedReporter, tokens::getInternalTokenById, cursorTracer );
    }

    static <RECORD extends AbstractBaseRecord,TOKEN extends TokenRecord> boolean checkValidToken(
            RECORD entity, int token, TokenHolder tokens, TokenStore<TOKEN> tokenStore, BiConsumer<RECORD,Integer> illegalTokenReport,
            BiConsumer<RECORD,TOKEN> unusedReporter, PageCursorTracer cursorTracer )
    {
        return checkValidToken( entity, token, tokens, tokenStore, illegalTokenReport, unusedReporter, tokens::getTokenById, cursorTracer );
    }

    private static <RECORD extends AbstractBaseRecord,TOKEN extends TokenRecord> boolean checkValidToken(
            RECORD entity, int token, TokenHolder tokens, TokenStore<TOKEN> tokenStore, BiConsumer<RECORD,Integer> illegalTokenReport,
            BiConsumer<RECORD,TOKEN> unusedReporter, ThrowingIntFunction<NamedToken,TokenNotFoundException> tokenGetter, PageCursorTracer cursorTracer )
    {
        if ( token < 0 )
        {
            illegalTokenReport.accept( entity, token );
            return false;
        }
        else
        {
            try
            {
                tokens.getTokenById( token );
                // It's in use, good
            }
            catch ( TokenNotFoundException tnfe )
            {
                TOKEN tokenRecord = tokenStore.getRecord( token, tokenStore.newRecord(), RecordLoad.FORCE, cursorTracer );
                unusedReporter.accept( entity, tokenRecord );
                return false;
            }
            // Regardless of whether or not it's in use apparently we're expected to count it
            return true;
        }
    }

    static void lightClear( MutableLongCollection collection )
    {
        if ( !collection.isEmpty() )
        {
            collection.clear();
        }
    }

    static void lightClear( MutableIntCollection collection )
    {
        if ( !collection.isEmpty() )
        {
            collection.clear();
        }
    }

    static void lightClear( MutablePrimitiveObjectMap<?> collection )
    {
        if ( !collection.isEmpty() )
        {
            collection.clear();
        }
    }
}
