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
package org.neo4j.consistency.repair;

import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

public class RelationshipChainExplorerTest
{
    private static final int NDegreeTwoNodes = 10;

    private final TestDirectory storeLocation = TestDirectory.testDirectory();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @ClassRule
    public static PageCacheRule pageCacheRule = new PageCacheRule();
    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( storeLocation ).around( fileSystemRule );

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
    public void shouldLoadAllConnectedRelationshipRecordsAndTheirFullChainsOfRelationshipRecords()
    {
        // given
        RecordStore<RelationshipRecord> relationshipStore = store.getRelationshipStore();

        // when
        int relationshipIdInMiddleOfChain = 10;
        RecordSet<RelationshipRecord> records = new RelationshipChainExplorer( relationshipStore )
                .exploreRelationshipRecordChainsToDepthTwo(
                        relationshipStore.getRecord( relationshipIdInMiddleOfChain, relationshipStore.newRecord(), NORMAL ) );

        // then
        assertEquals( NDegreeTwoNodes * 2, records.size() );
    }

    @Test
    public void shouldCopeWithAChainThatReferencesNotInUseZeroValueRecords()
    {
        // given
        RecordStore<RelationshipRecord> relationshipStore = store.getRelationshipStore();
        breakTheChain( relationshipStore );

        // when
        int relationshipIdInMiddleOfChain = 10;
        RecordSet<RelationshipRecord> records = new RelationshipChainExplorer( relationshipStore )
                .exploreRelationshipRecordChainsToDepthTwo(
                        relationshipStore.getRecord( relationshipIdInMiddleOfChain, relationshipStore.newRecord(), NORMAL ) );

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
        GraphDatabaseService database = new TestGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( storeDirectory )
                .setConfig( GraphDatabaseSettings.record_format, getRecordFormatName() )
                .setConfig( "dbms.backup.enabled", "false" )
                .newGraphDatabase();

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
        PageCache pageCache = pageCacheRule.getPageCache( fileSystemRule.get() );
        StoreAccess storeAccess = new StoreAccess( fileSystemRule.get(), pageCache, storeDirectory,
                Config.defaults() );
        return storeAccess.initialize();
    }

    protected String getRecordFormatName()
    {
        return StringUtils.EMPTY;
    }
}
