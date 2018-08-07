/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import java.util.List;
import java.util.Set;

import org.neo4j.consistency.checking.ChainCheck;
import org.neo4j.consistency.checking.CheckerEngine;
import org.neo4j.consistency.checking.RecordCheck;
import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.checking.index.IndexAccessors;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.impl.api.LookupFilter;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;

/**
 * Checks nodes and how they're indexed in one go. Reports any found inconsistencies.
 */
public class PropertyAndNodeIndexedCheck implements RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport>
{
    private final IndexAccessors indexes;
    private final NodePropertyReader propertyReader;
    private final CacheAccess cacheAccess;

    public PropertyAndNodeIndexedCheck( IndexAccessors indexes, NodePropertyReader propertyReader, CacheAccess cacheAccess )
    {
        this.indexes = indexes;
        this.propertyReader = propertyReader;
        this.cacheAccess = cacheAccess;
    }

    @Override
    public void check( NodeRecord record,
                       CheckerEngine<NodeRecord, ConsistencyReport.NodeConsistencyReport> engine,
                       RecordAccess records )
    {
        Collection<PropertyRecord> properties = propertyReader.getPropertyRecordChain( record );
        cacheAccess.client().putPropertiesToCache(properties);
        if ( indexes != null )
        {
            matchIndexesToNode( record, engine, records, properties );
        }
        checkProperty( record, engine, properties );
    }

    /**
     * Matches indexes to a node.
     */
    private void matchIndexesToNode(
            NodeRecord record,
            CheckerEngine<NodeRecord,
            ConsistencyReport.NodeConsistencyReport> engine,
            RecordAccess records,
            Collection<PropertyRecord> propertyRecs )
    {
        Set<Long> labels = NodeLabelReader.getListOfLabels( record, records, engine );
        IntObjectMap<PropertyBlock> nodePropertyMap = null;
        for ( StoreIndexDescriptor indexRule : indexes.onlineRules() )
        {
            long labelId = indexRule.schema().keyId();
            if ( labels.contains( labelId ) )
            {
                if ( nodePropertyMap == null )
                {
                    nodePropertyMap = properties( propertyReader.propertyBlocks( propertyRecs ) );
                }

                int[] indexPropertyIds = indexRule.schema().getPropertyIds();
                if ( nodeHasSchemaProperties( nodePropertyMap, indexPropertyIds ) )
                {
                    Value[] values = getPropertyValues( nodePropertyMap, indexPropertyIds );
                    try ( IndexReader reader = indexes.accessorFor( indexRule ).newReader() )
                    {
                        long nodeId = record.getId();

                        if ( indexRule.canSupportUniqueConstraint() )
                        {
                            verifyNodeCorrectlyIndexedUniquely( nodeId, values, engine, indexRule, reader );
                        }
                        else
                        {
                            long count = reader.countIndexedNodes( nodeId, values );
                            reportIncorrectIndexCount( values, engine, indexRule, count );
                        }
                    }
                }
            }
        }
    }

    private void verifyNodeCorrectlyIndexedUniquely( long nodeId, Value[] propertyValues,
            CheckerEngine<NodeRecord,ConsistencyReport.NodeConsistencyReport> engine, StoreIndexDescriptor indexRule,
            IndexReader reader )
    {
        IndexQuery[] query = seek( indexRule.schema(), propertyValues );

        final LongIterator indexedNodeIds = queryIndexOrEmpty( reader, query );

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
                engine.report().uniqueIndexNotUnique( indexRule, Values.asObjects( propertyValues ), indexedNodeId );
            }
        }

        reportIncorrectIndexCount( propertyValues, engine, indexRule, count );
    }

    private void reportIncorrectIndexCount( Value[] propertyValues,
                                            CheckerEngine<NodeRecord,ConsistencyReport.NodeConsistencyReport> engine,
                                            StoreIndexDescriptor indexRule,
                                            long count )
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

    private Value[] getPropertyValues( IntObjectMap<PropertyBlock> propertyMap, int[] indexPropertyIds )
    {
        Value[] values = new Value[indexPropertyIds.length];
        for ( int i = 0; i < indexPropertyIds.length; i++ )
        {
            PropertyBlock propertyBlock = propertyMap.get( indexPropertyIds[i] );
            values[i] = propertyReader.propertyValue( propertyBlock );
        }
        return values;
    }

    private IntObjectMap<PropertyBlock> properties( List<PropertyBlock> propertyBlocks )
    {
        final MutableIntObjectMap<PropertyBlock> propertyIds = new IntObjectHashMap<>();
        for ( PropertyBlock propertyBlock : propertyBlocks )
        {
            propertyIds.put( propertyBlock.getKeyIndexId(), propertyBlock );
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

    private LongIterator queryIndexOrEmpty( IndexReader reader, IndexQuery[] query )
    {
        final LongIterator indexedNodeIds;
        try
        {
            indexedNodeIds = reader.query( query );
        }
        catch ( IndexNotApplicableKernelException e )
        {
            throw new RuntimeException( format(
                    "Consistency checking error: index provider does not support exact query %s",
                    Arrays.toString( query ) ), e );
        }

        return reader.hasFullValuePrecision( query )
                ? indexedNodeIds : LookupFilter.exactIndexMatches( propertyReader, indexedNodeIds, query );
    }

    private static boolean nodeHasSchemaProperties(
            IntObjectMap<PropertyBlock> nodePropertyMap, int[] indexPropertyIds )
    {
        for ( int indexPropertyId : indexPropertyIds )
        {
            if ( !nodePropertyMap.containsKey( indexPropertyId ) )
            {
                return false;
            }
        }
        return true;
    }
}
