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
package org.neo4j.consistency.checking.full;

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import java.util.Arrays;
import java.util.Collection;

import org.neo4j.collection.PrimitiveLongResourceCollections;
import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.common.EntityType;
import org.neo4j.consistency.checking.ChainCheck;
import org.neo4j.consistency.checking.CheckerEngine;
import org.neo4j.consistency.checking.RecordCheck;
import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.checking.index.IndexAccessors;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.PropertySchemaType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.impl.api.LookupFilter;
import org.neo4j.kernel.impl.index.schema.NodeValueIterator;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.internal.kernel.api.QueryContext.NULL_CONTEXT;
import static org.neo4j.values.storable.NoValue.NO_VALUE;

/**
 * Checks nodes and how they're indexed in one go. Reports any found inconsistencies.
 */
public class PropertyAndNodeIndexedCheck implements RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport>
{
    private final IndexAccessors indexes;
    private final PropertyReader propertyReader;
    private final CacheAccess cacheAccess;

    PropertyAndNodeIndexedCheck( IndexAccessors indexes, PropertyReader propertyReader, CacheAccess cacheAccess )
    {
        this.indexes = indexes;
        this.propertyReader = propertyReader;
        this.cacheAccess = cacheAccess;
    }

    @Override
    public void check( NodeRecord record,
                       CheckerEngine<NodeRecord, ConsistencyReport.NodeConsistencyReport> engine,
                       RecordAccess records, PageCursorTracer cursorTracer )
    {
        try
        {
            Collection<PropertyRecord> properties = propertyReader.getPropertyRecordChain( record.getNextProp(), cursorTracer );
            cacheAccess.client().putPropertiesToCache( properties );
            if ( indexes != null )
            {
                matchIndexesToNode( record, engine, records, properties, cursorTracer );
            }
            checkProperty( record, engine, properties );
        }
        catch ( PropertyReader.CircularPropertyRecordChainException e )
        {
            engine.report().propertyChainContainsCircularReference( e.propertyRecordClosingTheCircle() );
        }
    }

    /**
     * Matches indexes to a node.
     */
    private void matchIndexesToNode( NodeRecord record, CheckerEngine<NodeRecord,ConsistencyReport.NodeConsistencyReport> engine, RecordAccess records,
            Collection<PropertyRecord> propertyRecs, PageCursorTracer cursorTracer )
    {
        long[] labels = NodeLabelReader.getListOfLabels( record, records, engine, cursorTracer ).stream().mapToLong( Long::longValue ).toArray();
        IntObjectMap<PropertyBlock> nodePropertyMap = null;
        for ( IndexDescriptor indexRule : indexes.onlineRules() )
        {
            SchemaDescriptor schema = indexRule.schema();
            if ( schema.entityType() == EntityType.NODE && schema.isAffected( labels ) )
            {
                if ( nodePropertyMap == null )
                {
                    nodePropertyMap = properties( propertyRecs );
                }

                if ( entityIntersectsSchema( nodePropertyMap, schema ) )
                {
                    Value[] values = getPropertyValues( propertyReader, nodePropertyMap, schema.getPropertyIds(), cursorTracer );
                    try ( IndexReader reader = indexes.accessorFor( indexRule ).newReader() )
                    {
                        long nodeId = record.getId();

                        if ( indexRule.isUnique() )
                        {
                            verifyNodeCorrectlyIndexedUniquely( nodeId, values, engine, indexRule, reader, cursorTracer );
                        }
                        else
                        {
                            long count = reader.countIndexedNodes( nodeId, cursorTracer, schema.getPropertyIds(), values );
                            reportIncorrectIndexCount( values, engine, indexRule, count );
                        }
                    }
                }
            }
        }
    }

    private void verifyNodeCorrectlyIndexedUniquely( long nodeId, Value[] propertyValues,
            CheckerEngine<NodeRecord,ConsistencyReport.NodeConsistencyReport> engine, IndexDescriptor descriptor,
            IndexReader reader, PageCursorTracer cursorTracer )
    {
        IndexQuery[] query = seek( descriptor.schema(), propertyValues );

        try ( PrimitiveLongResourceIterator indexedNodeIds = queryIndexOrEmpty( reader, query, cursorTracer ) )
        {
            long count = 0;
            while ( indexedNodeIds.hasNext() )
            {
                long indexedNodeId = indexedNodeIds.next();

                if ( nodeId == indexedNodeId )
                {
                    count++;
                }
                else
                {
                    engine.report().uniqueIndexNotUnique( descriptor, Values.asObjects( propertyValues ), indexedNodeId );
                }
            }

            reportIncorrectIndexCount( propertyValues, engine, descriptor, count );
        }
    }

    private void reportIncorrectIndexCount(
            Value[] propertyValues, CheckerEngine<NodeRecord,ConsistencyReport.NodeConsistencyReport> engine, IndexDescriptor indexRule, long count )
    {
        if ( count == 0 )
        {
            engine.report().notIndexed( indexRule, Values.asObjects( propertyValues ) );
        }
        else if ( count != 1 )
        {
            engine.report().indexedMultipleTimes( indexRule, Values.asObjects( propertyValues ), count );
        }
    }

    private void checkProperty( NodeRecord record,
            CheckerEngine<NodeRecord, ConsistencyReport.NodeConsistencyReport> engine,
            Collection<PropertyRecord> props )
    {
        if ( !Record.NO_NEXT_PROPERTY.is( record.getNextProp() ) )
        {
            PropertyRecord firstProp = props.iterator().next();
            if ( !Record.NO_PREVIOUS_PROPERTY.is( firstProp.getPrevProp() ) )
            {
                engine.report().propertyNotFirstInChain( firstProp );
            }

            final MutableIntSet keys = new IntHashSet();
            for ( PropertyRecord property : props )
            {
                if ( !property.inUse() )
                {
                    engine.report().propertyNotInUse( property );
                }
                else
                {
                    for ( int key : ChainCheck.keys( property ) )
                    {
                        if ( !keys.add( key ) )
                        {
                            engine.report().propertyKeyNotUniqueInChain();
                        }
                    }
                }
            }
        }
    }

    static Value[] getPropertyValues( PropertyReader propertyReader, IntObjectMap<PropertyBlock> propertyMap, int[] indexPropertyIds,
            PageCursorTracer cursorTracer )
    {
        Value[] values = new Value[indexPropertyIds.length];
        for ( int i = 0; i < indexPropertyIds.length; i++ )
        {
            PropertyBlock propertyBlock = propertyMap.get( indexPropertyIds[i] );
            values[i] = propertyBlock != null ? propertyReader.propertyValue( propertyBlock, cursorTracer ) : NO_VALUE;
        }
        return values;
    }

    static IntObjectMap<PropertyBlock> properties( Collection<PropertyRecord> records )
    {
        final MutableIntObjectMap<PropertyBlock> propertyIds = new IntObjectHashMap<>();
        for ( PropertyRecord record : records )
        {
            for ( PropertyBlock propertyBlock : record )
            {
                propertyIds.put( propertyBlock.getKeyIndexId(), propertyBlock );
            }
        }
        return propertyIds;
    }

    private IndexQuery[] seek( SchemaDescriptor schema, Value[] propertyValues )
    {
        int[] propertyIds = schema.getPropertyIds();
        assert propertyIds.length == propertyValues.length;
        IndexQuery[] query = new IndexQuery[propertyValues.length];
        for ( int i = 0; i < query.length; i++ )
        {
            query[i] = IndexQuery.exact( propertyIds[i], propertyValues[i] );
        }
        return query;
    }

    private PrimitiveLongResourceIterator queryIndexOrEmpty( IndexReader reader, IndexQuery[] query, PageCursorTracer cursorTracer )
    {
        final PrimitiveLongResourceIterator indexedNodeIds;
        try
        {
            NodeValueIterator iterator = new NodeValueIterator();
            reader.query( NULL_CONTEXT, iterator, unconstrained(), cursorTracer, query );
            indexedNodeIds = iterator;
        }
        catch ( IndexNotApplicableKernelException e )
        {
            throw new RuntimeException( format(
                    "Consistency checking error: index provider does not support exact query %s",
                    Arrays.toString( query ) ), e );
        }

        if ( reader.hasFullValuePrecision( query ) )
        {
            return indexedNodeIds;
        }
        LongIterator filtered = LookupFilter.exactIndexMatches( propertyReader, indexedNodeIds, cursorTracer, query );
        return new PrimitiveLongResourceCollections.AbstractPrimitiveLongBaseResourceIterator( indexedNodeIds )
        {
            @Override
            protected boolean fetchNext()
            {
                return filtered.hasNext() && next( filtered.next() );
            }
        };
    }

    static boolean entityIntersectsSchema( IntObjectMap<PropertyBlock> entityPropertyMap, SchemaDescriptor schema )
    {
        boolean requireAllTokens = schema.propertySchemaType() == PropertySchemaType.COMPLETE_ALL_TOKENS;
        if ( requireAllTokens )
        {
            return hasAllProperties( entityPropertyMap, schema.getPropertyIds() );
        }
        else
        {
            return hasAnyProperty( entityPropertyMap, schema.getPropertyIds() );
        }
    }

    private static boolean hasAllProperties( IntObjectMap<PropertyBlock> blockMap, int[] indexPropertyIds )
    {
        for ( int indexPropertyId : indexPropertyIds )
        {
            if ( !blockMap.containsKey( indexPropertyId ) )
            {
                return false;
            }
        }
        return true;
    }

    private static boolean hasAnyProperty( IntObjectMap<PropertyBlock> blockMap, int[] indexPropertyIds )
    {
        for ( int indexPropertyId : indexPropertyIds )
        {
            if ( blockMap.containsKey( indexPropertyId ) )
            {
                return true;
            }
        }
        return false;
    }
}
