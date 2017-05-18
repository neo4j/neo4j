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
package org.neo4j.unsafe.impl.batchimport;

import org.junit.Test;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.id.IdGeneratorImpl;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache;
import org.neo4j.unsafe.impl.batchimport.cache.NodeType;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitors;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionSupervisors;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RelationshipLinkbackStageTest
{
    @Test
    public void reservedIdIsSkipped() throws Exception
    {
        long highId = 5;
        RelationshipStore store = newRelationshipStoreMock( highId );
        RelationshipLinkbackStage stage = new RelationshipLinkbackStage( "Test",
                Configuration.DEFAULT, store, newCache(), null, null, NodeType.NODE_TYPE_SPARSE );

        ExecutionSupervisors.superviseExecution( ExecutionMonitors.invisible(), Configuration.DEFAULT, stage );

        verify( store, never() ).updateRecord( new RelationshipRecord( IdGeneratorImpl.INTEGER_MINUS_ONE ) );
    }

    private static RelationshipStore newRelationshipStoreMock( long highId )
    {
        RelationshipStore store = mock( RelationshipStore.class );
        when( store.getHighId() ).thenReturn( highId );
        RelationshipRecord record = new RelationshipRecord( -1 );
        when( store.newRecord() ).thenReturn( record );
        PageCursor pageCursor = mock( PageCursor.class );
        when( store.newPageCursor() ).thenReturn( pageCursor );
        when( store.readRecord( anyInt(), eq( record ), any( RecordLoad.class ), eq( pageCursor ) ) )
                .thenAnswer( invocation ->
                {
                    long id = (long) invocation.getArguments()[0];
                    long realId = (id == highId - 1) ? IdGeneratorImpl.INTEGER_MINUS_ONE : id;
                    record.setId( realId );
                    record.setInUse( true );
                    record.setFirstNode( 0 );
                    record.setSecondNode( 0 );
                    return record;
                } );

        return store;
    }

    private static NodeRelationshipCache newCache()
    {
        int denseNodeThreshold = Integer.parseInt( GraphDatabaseSettings.dense_node_threshold.getDefaultValue() );
        NodeRelationshipCache nodeRelationshipCache =
                new NodeRelationshipCache( NumberArrayFactory.HEAP, denseNodeThreshold );
        nodeRelationshipCache.setHighNodeId( 1 );
        return  nodeRelationshipCache;
    }
}
