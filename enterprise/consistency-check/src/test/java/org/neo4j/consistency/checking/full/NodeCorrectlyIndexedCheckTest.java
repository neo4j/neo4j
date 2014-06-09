/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.consistency.checking.full;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.consistency.checking.CheckerEngine;
import org.neo4j.consistency.checking.index.IndexAccessors;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.api.direct.BoundedIterable;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.SchemaIndexProvider.Descriptor;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.labels.NodeLabelsField;

import static java.util.Arrays.asList;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.emptyIterator;
import static org.neo4j.kernel.api.properties.Property.stringProperty;
import static org.neo4j.kernel.impl.nioneo.store.IndexRule.constraintIndexRule;
import static org.neo4j.kernel.impl.nioneo.store.IndexRule.indexRule;

public class NodeCorrectlyIndexedCheckTest
{
    private static final int indexId = 12;
    private static final int labelId = 34;
    private static final int propertyKeyId = 56;
    private static final long constraintId = 78;

    @Test
    public void shouldBeSilentWhenNodesCorrectlyIndexed() throws Exception
    {
        // given
        IndexRule indexRule = indexRule( indexId, labelId, propertyKeyId, new Descriptor( "provider1", "version1" ) );
        NodeRecord nodeRecord = nodeWithLabels( labelId );

        NodeCorrectlyIndexedCheck check = new NodeCorrectlyIndexedCheck(
                indexContaining( indexRule, Collections.<Object, long[]>singletonMap(
                        "propertyValue", new long[]{nodeRecord.getId()} ) ),
                nodeHasProperty( nodeRecord, "propertyValue" ) );

        ConsistencyReport.NodeConsistencyReport report = mock( ConsistencyReport.NodeConsistencyReport.class );

        // when
        check.check( nodeRecord, engineFor( report ), null );

        // then
        verifyZeroInteractions( report );
    }

    @Test
    public void shouldReportNodeThatIsNotIndexed() throws Exception
    {
        // given
        IndexRule indexRule = indexRule( indexId, labelId, propertyKeyId, new Descriptor( "provider1", "version1" ) );
        NodeRecord nodeRecord = nodeWithLabels( labelId );

        NodeCorrectlyIndexedCheck check = new NodeCorrectlyIndexedCheck(
                indexContaining( indexRule, new HashMap<Object, long[]>() ),
                nodeHasProperty( nodeRecord, "propertyValue" ) );

        ConsistencyReport.NodeConsistencyReport report = mock( ConsistencyReport.NodeConsistencyReport.class );

        // when
        check.check( nodeRecord, engineFor( report ), null );

        // then
        verify( report ).notIndexed( indexRule, "propertyValue" );
    }

    @Test
    public void shouldReportDuplicateNode() throws Exception
    {
        // given
        IndexRule indexRule = constraintIndexRule( indexId, labelId, propertyKeyId,
                new Descriptor( "provider1", "version1" ), constraintId );
        NodeRecord nodeRecord = nodeWithLabels( labelId );
        long duplicateNodeId1 = 1;
        long duplicateNodeId2 = 2;

        NodeCorrectlyIndexedCheck check = new NodeCorrectlyIndexedCheck(
                indexContaining( indexRule, Collections.<Object, long[]>singletonMap(
                        "propertyValue", new long[]{nodeRecord.getId(), duplicateNodeId1, duplicateNodeId2} ) ),
                nodeHasProperty( nodeRecord, "propertyValue" ) );

        ConsistencyReport.NodeConsistencyReport report = mock( ConsistencyReport.NodeConsistencyReport.class );

        // when
        check.check( nodeRecord, engineFor( report ), null );

        // then
        verify( report ).uniqueIndexNotUnique( indexRule, "propertyValue", duplicateNodeId1 );
        verify( report ).uniqueIndexNotUnique( indexRule, "propertyValue", duplicateNodeId2 );
    }

    private IndexAccessors indexContaining( IndexRule indexRule, Map<Object, long[]> entries ) throws IOException
    {
        IndexAccessorStub reader = new IndexAccessorStub( entries );
        IndexAccessors indexes = mock( IndexAccessors.class );
        when( indexes.accessorFor( any( IndexRule.class ) ) )
                .thenReturn( reader );
        when (indexes.rules() )
                .thenReturn( asList(indexRule) );
        return indexes;
    }

    private PropertyReader nodeHasProperty( NodeRecord nodeRecord, String propertyValue )
    {
        PropertyReader propertyReader = mock( PropertyReader.class );
        PropertyBlock propertyBlock = mock( PropertyBlock.class );
        when( propertyBlock.getKeyIndexId() )
                .thenReturn( propertyKeyId );

        when( propertyReader.propertyBlocks( nodeRecord ) )
                .thenReturn( asList( propertyBlock ) );
        when( propertyReader.propertyValue( any( PropertyBlock.class ) ) )
                .thenReturn( stringProperty( propertyKeyId, propertyValue ) );
        return propertyReader;
    }

    private NodeRecord nodeWithLabels( long... labelIds )
    {
        NodeRecord nodeRecord = new NodeRecord( 0, false, 0, 0 );
        NodeLabelsField.parseLabelsField( nodeRecord ).put( labelIds, null, null );
        return nodeRecord;
    }

    @SuppressWarnings("unchecked")
    private CheckerEngine<NodeRecord, ConsistencyReport.NodeConsistencyReport>
    engineFor( ConsistencyReport.NodeConsistencyReport report )
    {
        CheckerEngine engine = mock( CheckerEngine.class );
        when( engine.report() ).thenReturn( report );
        return engine;
    }

    private static class IndexAccessorStub implements IndexAccessor
    {
        private final Map<Object, long[]> entries;

        private IndexAccessorStub( Map<Object, long[]> entries )
        {
            this.entries = entries;
        }

        @Override
        public IndexReader newReader()
        {
            return new IndexReader()
            {
                @Override
                public PrimitiveLongIterator lookup( Object value )
                {
                    if ( entries.containsKey( value ) )
                    {
                        return PrimitiveLongCollections.iterator( entries.get( value ) );
                    }
                    return emptyIterator();
                }

                @Override
                public boolean hasIndexed( long nodeId, Object propertyValue )
                {
                    long[] canidates = entries.get( propertyValue );
                    if ( canidates == null )
                    {
                        return false;
                    }
                    for ( int i = 0; i < canidates.length; i++ )
                    {
                        if ( canidates[i] == nodeId )
                        {
                            return true;
                        }
                    }
                    return false;
                }

                @Override
                public void close()
                {
                }
            };
        }

        @Override
        public void close() throws IOException
        {
        }

        @Override
        public void drop() throws IOException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public IndexUpdater newUpdater( IndexUpdateMode mode )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void force() throws IOException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public BoundedIterable<Long> newAllEntriesReader()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ResourceIterator<File> snapshotFiles() throws IOException
        {
            throw new UnsupportedOperationException();
        }
    }
}
