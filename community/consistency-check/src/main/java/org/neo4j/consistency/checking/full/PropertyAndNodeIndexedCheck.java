/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.consistency.checking.full;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongPeekingIterator;
import org.neo4j.consistency.checking.ChainCheck;
import org.neo4j.consistency.checking.CheckerEngine;
import org.neo4j.consistency.checking.RecordCheck;
import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.checking.index.IndexAccessors;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.impl.api.LookupFilter;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;

/**
 * Checks nodes and how they're indexed in one go. Reports any found inconsistencies.
 */
public class PropertyAndNodeIndexedCheck implements RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport>
{
    private final IndexAccessors indexes;
    private final PropertyReader propertyReader;
    private final CacheAccess cacheAccess;

    public PropertyAndNodeIndexedCheck( IndexAccessors indexes,
                                      PropertyReader propertyReader, CacheAccess cacheAccess )
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
            checkIndexToLabels( record, engine, records, properties );
        }
        checkProperty( record, engine, properties );
    }

    private void checkIndexToLabels( NodeRecord record,
            CheckerEngine<NodeRecord, ConsistencyReport.NodeConsistencyReport> engine,
            RecordAccess records,
            Collection<PropertyRecord> propertyRecs)
    {
        Set<Long> labels = NodeLabelReader.getListOfLabels( record, records, engine );
        List<PropertyBlock> properties = null;
        for ( IndexRule indexRule : indexes.rules() )
        {
            if ( !labels.contains( (long) indexRule.getLabel() ) )
            {
                continue;
            }

            if ( properties == null )
            {
                properties = propertyReader.propertyBlocks( propertyRecs );
            }
            PropertyBlock property = propertyWithKey( properties, indexRule.getPropertyKey() );

            if ( property == null )
            {
                continue;
            }

            try ( IndexReader reader = indexes.accessorFor( indexRule ).newReader() )
            {
                Object propertyValue = propertyReader.propertyValue( property ).value();
                long nodeId = record.getId();

                if ( indexRule.isConstraintIndex() )
                {
                    verifyNodeCorrectlyIndexedUniquely( nodeId, property.getKeyIndexId(), propertyValue, engine,
                            indexRule, reader );
                }
                else
                {
                    verifyNodeCorrectlyIndexed( nodeId, propertyValue, engine, indexRule, reader );
                }
            }
        }
    }

    private void verifyNodeCorrectlyIndexedUniquely( long nodeId, int propertyKeyId, Object propertyValue,
            CheckerEngine<NodeRecord,ConsistencyReport.NodeConsistencyReport> engine, IndexRule indexRule,
            IndexReader reader )
    {
        PrimitiveLongIterator indexedNodeIds = reader.seek( propertyValue );

        // For verifying node indexed uniquely in offline CC, if one match found in the first stage match,
        // then there is no need to filter the result. The result is a exact match.
        // If multiple matches found, we need to filter the result to get exact matches.
        indexedNodeIds = filterIfMultipleValuesFound( indexedNodeIds, propertyKeyId, propertyValue );

        boolean found = false;

        while ( indexedNodeIds.hasNext() )
        {
            long indexedNodeId = indexedNodeIds.next();

            if ( nodeId == indexedNodeId )
            {
                found = true;
            }
            else
            {
                engine.report().uniqueIndexNotUnique( indexRule, propertyValue, indexedNodeId );
            }
        }

        if ( !found )
        {
            engine.report().notIndexed( indexRule, propertyValue );
        }
    }

    private PrimitiveLongIterator filterIfMultipleValuesFound( PrimitiveLongIterator indexedNodeIds, int propertyKeyId,
            Object propertyValue )
    {
        PrimitiveLongIterator filteredIndexedNodeIds = new PrimitiveLongPeekingIterator( indexedNodeIds );
        if ( ((PrimitiveLongPeekingIterator) filteredIndexedNodeIds).hasMultipleValues() )
        {
            filteredIndexedNodeIds = LookupFilter.exactIndexMatches( propertyReader, filteredIndexedNodeIds,
                    propertyKeyId, propertyValue );
        }
        return filteredIndexedNodeIds;
    }

    private void verifyNodeCorrectlyIndexed(
            long nodeId,
            Object propertyValue,
            CheckerEngine<NodeRecord, ConsistencyReport.NodeConsistencyReport> engine,
            IndexRule indexRule,
            IndexReader reader )
    {
        int count = reader.countIndexedNodes( nodeId, propertyValue );
        if ( count == 0 )
        {
            engine.report().notIndexed( indexRule, propertyValue );
        }
        else if ( count == 1 )
        {   // Nothing to report, all good
        }
        else
        {
            engine.report().indexedMultipleTimes( indexRule, propertyValue, count );
        }
    }

    private PropertyBlock propertyWithKey( List<PropertyBlock> propertyBlocks, int propertyKey )
    {
        for ( PropertyBlock propertyBlock : propertyBlocks )
        {
            if ( propertyBlock.getKeyIndexId() == propertyKey )
            {
                return propertyBlock;
            }
        }
        return null;
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

            PrimitiveIntSet keys = Primitive.intSet();
            for (PropertyRecord property : props)
            {
                if ( !property.inUse() )
                {
                    engine.report().propertyNotInUse( property );
                }
                else
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
