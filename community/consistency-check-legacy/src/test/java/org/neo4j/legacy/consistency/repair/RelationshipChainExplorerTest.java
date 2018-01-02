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
package org.neo4j.legacy.consistency.repair;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;

public class RelationshipChainExplorerTest
{
    private static final int NDegreeTwoNodes = 10;

    @ClassRule
    public static PageCacheRule pageCacheRule = new PageCacheRule();
    @Rule
    public TargetDirectory.TestDirectory storeLocation = TargetDirectory.testDirForTest( getClass() );
    private StoreAccess store;

    @Before
    public void setupStoreAccess()
    {
        store = createStoreWithOneHighDegreeNodeAndSeveralDegreeTwoNodes( NDegreeTwoNodes );
    }

    @After
    public void tearDownStoreAccess()
    {
        store.close();
    }

    @Test
    public void shouldLoadAllConnectedRelationshipRecordsAndTheirFullChainsOfRelationshipRecords() throws Exception
    {
        // given
        RecordStore<RelationshipRecord> relationshipStore = store.getRelationshipStore();

        // when
        int relationshipIdInMiddleOfChain = 10;
        RecordSet<RelationshipRecord> records = new RelationshipChainExplorer( relationshipStore )
                .exploreRelationshipRecordChainsToDepthTwo(
                        relationshipStore.getRecord( relationshipIdInMiddleOfChain ) );

        // then
        assertEquals( NDegreeTwoNodes * 2, records.size() );
    }

    @Test
    public void shouldCopeWithAChainThatReferencesNotInUseZeroValueRecords() throws Exception
    {
        // given
        RecordStore<RelationshipRecord> relationshipStore = store.getRelationshipStore();
        breakTheChain( relationshipStore );

        // when
        int relationshipIdInMiddleOfChain = 10;
        RecordSet<RelationshipRecord> records = new RelationshipChainExplorer( relationshipStore )
                .exploreRelationshipRecordChainsToDepthTwo(
                        relationshipStore.getRecord( relationshipIdInMiddleOfChain ) );

        // then
        int recordsInaccessibleBecauseOfBrokenChain = 3;
        assertEquals( NDegreeTwoNodes * 2 - recordsInaccessibleBecauseOfBrokenChain, records.size() );
    }

    private void breakTheChain( RecordStore<RelationshipRecord> relationshipStore )
    {
        int relationshipTowardsEndOfChain = 16;
        relationshipStore.updateRecord( new RelationshipRecord( relationshipTowardsEndOfChain, 0, 0, 0 ) );
    }

    enum TestRelationshipType implements RelationshipType
    {
        CONNECTED
    }

    private StoreAccess createStoreWithOneHighDegreeNodeAndSeveralDegreeTwoNodes( int nDegreeTwoNodes )
    {
        File storeDirectory = storeLocation.graphDbDir();
        GraphDatabaseService database = new TestGraphDatabaseFactory().newEmbeddedDatabase( storeDirectory );

        try ( Transaction transaction = database.beginTx() )
        {
            Node denseNode = database.createNode();
            for ( int i = 0; i < nDegreeTwoNodes; i++ )
            {
                Node degreeTwoNode = database.createNode();
                Node leafNode = database.createNode();
                if ( i % 2 == 0 )
                {
                    denseNode.createRelationshipTo( degreeTwoNode, TestRelationshipType.CONNECTED );
                }
                else
                {
                    degreeTwoNode.createRelationshipTo( denseNode, TestRelationshipType.CONNECTED );
                }
                degreeTwoNode.createRelationshipTo( leafNode, TestRelationshipType.CONNECTED );
            }
            transaction.success();
        }
        database.shutdown();
        PageCache pageCache = pageCacheRule.getPageCache( new DefaultFileSystemAbstraction() );
        return new StoreAccess( pageCache, storeDirectory ).initialize();
    }
}
