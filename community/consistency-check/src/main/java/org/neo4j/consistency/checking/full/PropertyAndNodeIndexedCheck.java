/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.consistency.checking.ChainCheck;
import org.neo4j.consistency.checking.CheckerEngine;
import org.neo4j.consistency.checking.RecordCheck;
import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.checking.index.IndexAccessors;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.kernel.api.exceptions.index.IndexNotApplicableKernelException;
import org.neo4j.kernel.api.schema_new.IndexQuery;
import org.neo4j.kernel.api.schema_new.LabelSchemaDescriptor;
import org.neo4j.kernel.impl.api.LookupFilter;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.storageengine.api.schema.IndexReader;

import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_PROPERTY_KEY;
import static org.neo4j.kernel.impl.api.schema.NodeSchemaMatcher.nodeHasSchemaProperties;

/**
 * Checks nodes and how they're indexed in one go. Reports any found inconsistencies.
 */
public class PropertyAndNodeIndexedCheck implements RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport>
{
    private final IndexAccessors indexes;
    private final PropertyReader propertyReader;
    private final CacheAccess cacheAccess;

    public PropertyAndNodeIndexedCheck( IndexAccessors indexes, PropertyReader propertyReader, CacheAccess cacheAccess )
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
     * Matches indexes to a node. This implementation mirrors NodeSchemaMatcher.onMatchingSchema(...), but as all
     * accessor methods are different, a shared implementation was hard to achieve.
     */
    private void matchIndexesToNode(
            NodeRecord record,
            CheckerEngine<NodeRecord,
            ConsistencyReport.NodeConsistencyReport> engine,
            RecordAccess records,
            Collection<PropertyRecord> propertyRecs )
    {
        Set<Long> labels = NodeLabelReader.getListOfLabels( record, records, engine );
        List<PropertyBlock> properties = null;
        PrimitiveIntSet nodePropertyIds = null;
        for ( IndexRule indexRule : indexes.rules() )
        {
            long labelId = indexRule.schema().getLabelId();
            if ( labels.contains( labelId ) )
            {
                if ( properties == null )
                {
                    properties = propertyReader.propertyBlocks( propertyRecs );
                    nodePropertyIds = propertyIds( properties );
                }

                int[] indexPropertyIds = indexRule.schema().getPropertyIds();
                if ( nodeHasSchemaProperties( nodePropertyIds, indexPropertyIds, NO_SUCH_PROPERTY_KEY ) )
                {
                    Object[] values = getPropertyValues( properties, indexPropertyIds );
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

    private void verifyNodeCorrectlyIndexedUniquely( long nodeId, Object[] propertyValues,
            CheckerEngine<NodeRecord,ConsistencyReport.NodeConsistencyReport> engine, IndexRule indexRule,
            IndexReader reader )
    {
        IndexQuery[] query = seek( indexRule.schema(), propertyValues );

        PrimitiveLongIterator indexedNodeIds = queryIndexOrEmpty( reader, query );

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
                engine.report().uniqueIndexNotUnique( indexRule, propertyValues, indexedNodeId );
            }
        }

        reportIncorrectIndexCount( propertyValues, engine, indexRule, count );
    }

    private void reportIncorrectIndexCount( Object[] propertyValues,
            CheckerEngine<NodeRecord,ConsistencyReport.NodeConsistencyReport> engine, IndexRule indexRule, long count )
    {
        if ( count == 0 )
        {
            engine.report().notIndexed( indexRule, propertyValues );
        }
        else if ( count != 1 )
        {
            engine.report().indexedMultipleTimes( indexRule, propertyValues, count );
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

            PrimitiveIntSet keys = Primitive.intSet();
            for (PropertyRecord property : props)
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

    private Object[] getPropertyValues( List<PropertyBlock> properties, int[] indexPropertyIds )
    {
        Object[] values = new Object[indexPropertyIds.length];
        for ( int i = 0; i < indexPropertyIds.length; i++ )
        {
            PropertyBlock propertyBlock = propertyWithKey( properties, indexPropertyIds[i] );
            if ( propertyBlock == null )
            {
                throw new IllegalStateException( "We should have checked that the index and node match before coming " +
                        "here, so this should not happen" );
            }
            values[i] = propertyReader.propertyValue( propertyBlock ).value();
        }
        return values;
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

    private PrimitiveIntSet propertyIds( List<PropertyBlock> propertyBlocks )
    {
        PrimitiveIntSet propertyIds = Primitive.intSet();
        for ( PropertyBlock propertyBlock : propertyBlocks )
        {
            propertyIds.add( propertyBlock.getKeyIndexId() );
        }
        return propertyIds;
    }

    private IndexQuery[] seek( LabelSchemaDescriptor schema, Object[] propertyValues )
    {
        assert schema.getPropertyIds().length == propertyValues.length;
        IndexQuery[] query = new IndexQuery[propertyValues.length];
        for ( int i = 0; i < query.length; i++ )
        {
            query[i] = IndexQuery.exact( schema.getPropertyIds()[i], propertyValues[i] );
        }
        return query;
    }

    private PrimitiveLongIterator queryIndexOrEmpty( IndexReader reader, IndexQuery[] query )
    {
        PrimitiveLongIterator indexedNodeIds;
        try
        {
            indexedNodeIds = reader.query( query );
        }
        catch ( IndexNotApplicableKernelException e )
        {
            indexedNodeIds = PrimitiveLongCollections.emptyIterator();
        }

        indexedNodeIds = LookupFilter.exactIndexMatches( propertyReader, indexedNodeIds, query );
        return indexedNodeIds;
    }
}
