/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.transaction.state.storeview;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.lock.LockService;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.test.Race;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.token.TokenHolders;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.function.Predicates.ALWAYS_TRUE_INT;
import static org.neo4j.graphdb.IndexingTestUtil.assertOnlyDefaultTokenIndexesExists;
import static org.neo4j.graphdb.IndexingTestUtil.dropTokenIndexes;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@ExtendWith( RandomExtension.class )
@DbmsExtension
public class DynamicIndexStoreViewIT
{
    private static final Label PERSON = Label.label( "person" );
    private static final RelationshipType FRIEND = RelationshipType.withName( "friend" );

    @Inject
    private RandomSupport random;
    @Inject
    private GraphDatabaseAPI database;
    @Inject
    private LockService lockService;
    @Inject
    private Locks locks;
    @Inject
    private IndexingService indexingService;
    @Inject
    private StorageEngine storageEngine;
    @Inject
    private TokenHolders tokenHolders;
    @Inject
    private JobScheduler scheduler;

    private DynamicIndexStoreView storeView;

    @BeforeEach
    void setUp()
    {
        assertOnlyDefaultTokenIndexesExists( database );
        storeView = new DynamicIndexStoreView(
                new FullScanStoreView( lockService, storageEngine::newReader, storageEngine::createStorageCursors, Config.defaults(), scheduler ),
                locks, lockService, Config.defaults(), indexDescriptor -> indexingService.getIndexProxy( indexDescriptor ),
                storageEngine::newReader, storageEngine::createStorageCursors, NullLogProvider.nullLogProvider() );
    }

    @Disabled( "disabled until we have token indexes on by default" )
    @Test
    void shouldHandleConcurrentDeletionOfTokenIndexDuringNodeScan() throws Throwable
    {
        //Given
        var nodes = populateNodes();
        var consumer = new TestTokenScanConsumer();
        var storeScan = nodeStoreScan( consumer );

        //When
        Race race = new Race();
        race.addContestant( () -> storeScan.run( new ContainsExternalUpdates() ) );
        race.addContestant( () -> dropTokenIndexes( database ), 5 );
        race.go();

        //Then
        assertThat( storeScan.getProgress().getTotal() ).isEqualTo( nodes );
        assertThat( consumer.consumedEntities() ).isEqualTo( nodes );
        storeScan.stop();
    }

    @Disabled( "disabled until we have token indexes on by default" )
    @Test
    void shouldHandleConcurrentDeletionOfTokenIndexDuringRelationshipScan() throws Throwable
    {
        //Given
        var relationships = populateRelationships();
        var consumer = new TestTokenScanConsumer();
        var storeScan = relationshipStoreScan( consumer );

        //When
        Race race = new Race();
        race.addContestant( () -> storeScan.run( new ContainsExternalUpdates() ) );
        race.addContestant( () -> dropTokenIndexes( database ), 5 );
        race.go();

        //Then
        assertThat( storeScan.getProgress().getTotal() ).isEqualTo( relationships );
        assertThat( consumer.consumedEntities() ).isEqualTo( relationships );
        storeScan.stop();
    }

    private class ContainsExternalUpdates implements StoreScan.ExternalUpdatesCheck
    {
        @Override
        public boolean needToApplyExternalUpdates()
        {
            return random.nextBoolean();
        }

        @Override
        public void applyExternalUpdates( long id )
        {
        }
    }

    private StoreScan nodeStoreScan( TestTokenScanConsumer consumer )
    {
        return storeView.visitNodes(
                getLabelIds(), ALWAYS_TRUE_INT, null, consumer, false, true, new DefaultPageCacheTracer(), INSTANCE );
    }

    private StoreScan relationshipStoreScan( TestTokenScanConsumer consumer )
    {
        return storeView.visitRelationships(
                getRelationTypeIds(), ALWAYS_TRUE_INT, null, consumer, false, true, new DefaultPageCacheTracer(), INSTANCE );
    }

    private int[] getLabelIds()
    {
        return new int[]{tokenHolders.labelTokens().getIdByName( PERSON.name() )};
    }

    private int[] getRelationTypeIds()
    {
        return new int[]{tokenHolders.relationshipTypeTokens().getIdByName( FRIEND.name() )};
    }

    private long populateNodes()
    {
        long nodes = Configuration.DEFAULT.batchSize() + 100;
        try ( var tx = database.beginTx() )
        {
            for ( int i = 0; i < nodes; i++ )
            {
                tx.createNode( PERSON );
            }
            tx.commit();
        }
        return nodes;
    }

    private long populateRelationships()
    {
        long relations = Configuration.DEFAULT.batchSize() + 100;
        try ( var tx = database.beginTx() )
        {
            for ( int i = 0; i < relations; i++ )
            {
                tx.createNode( PERSON ).createRelationshipTo( tx.createNode( PERSON ), FRIEND );
            }
            tx.commit();
        }
        return relations;
    }
}
