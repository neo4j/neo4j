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
package org.neo4j.consistency.checking.full;

import org.apache.commons.lang3.StringUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.consistency.statistics.Statistics;
import org.neo4j.consistency.store.DirectStoreAccess;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.index.label.LabelScanStore;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class DetectAllRelationshipInconsistenciesIT
{
    private final TestDirectory directory = TestDirectory.testDirectory();
    private final RandomRule random = new RandomRule();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    @Rule
    public final RuleChain rules = RuleChain.outerRule( random ).around( directory ).around( fileSystemRule );
    private DatabaseManagementService managementService;

    @Test
    public void shouldDetectSabotagedRelationshipWhereEverItIs() throws Exception
    {
        // GIVEN a database which lots of relationships
        GraphDatabaseAPI db = getGraphDatabaseAPI();
        Sabotage sabotage;
        try
        {
            Node[] nodes = new Node[1_000];
            Relationship[] relationships = new Relationship[10_000];
            try ( Transaction tx = db.beginTx() )
            {
                for ( int i = 0; i < nodes.length; i++ )
                {
                    nodes[i] = db.createNode( label( "Foo" ) );
                }
                for ( int i = 0; i < 10_000; i++ )
                {
                    relationships[i] =
                            random.among( nodes ).createRelationshipTo( random.among( nodes ), MyRelTypes.TEST );
                }
                tx.success();
            }

            // WHEN sabotaging a random relationship
            DependencyResolver resolver = db.getDependencyResolver();
            NeoStores neoStores = resolver.resolveDependency( RecordStorageEngine.class ).testAccessNeoStores();
            RelationshipStore relationshipStore = neoStores.getRelationshipStore();
            Relationship sabotagedRelationships = random.among( relationships );
            sabotage = sabotage( relationshipStore, sabotagedRelationships.getId() );
        }
        finally
        {
            managementService.shutdown();
        }

        // THEN the checker should find it, where ever it is in the store
        db = getGraphDatabaseAPI();
        try
        {
            DependencyResolver resolver = db.getDependencyResolver();
            NeoStores neoStores = resolver.resolveDependency( RecordStorageEngine.class ).testAccessNeoStores();
            CountsTracker counts = resolver.resolveDependency( RecordStorageEngine.class ).testAccessCountsStore();
            StoreAccess storeAccess = new StoreAccess( neoStores ).initialize();
            DirectStoreAccess directStoreAccess = new DirectStoreAccess( storeAccess,
                    db.getDependencyResolver().resolveDependency( LabelScanStore.class ),
                    db.getDependencyResolver().resolveDependency( IndexProviderMap.class ), counts );

            int threads = random.intBetween( 2, 10 );
            FullCheck checker = new FullCheck( getTuningConfiguration(), ProgressMonitorFactory.NONE, Statistics.NONE, threads );
            AssertableLogProvider logProvider = new AssertableLogProvider( true );
            ConsistencySummaryStatistics summary = checker.execute( directStoreAccess,
                    logProvider.getLog( FullCheck.class ) );
            int relationshipInconsistencies = summary.getInconsistencyCountForRecordType(
                    RecordType.RELATIONSHIP );

            assertTrue( "Couldn't detect sabotaged relationship " + sabotage, relationshipInconsistencies > 0 );
            logProvider.assertContainsLogCallContaining( sabotage.after.toString() );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    private StoreFactory newStoreFactory( PageCache pageCache )
    {
        FileSystemAbstraction fileSystem = fileSystemRule.get();
        return new StoreFactory( directory.databaseLayout(), getTuningConfiguration(),
                new DefaultIdGeneratorFactory( fileSystem ), pageCache, fileSystem, NullLogProvider.getInstance() );
    }

    private Config getTuningConfiguration()
    {
        return Config.defaults( stringMap( GraphDatabaseSettings.pagecache_memory.name(), "8m",
                          GraphDatabaseSettings.record_format.name(), getRecordFormatName() ) );
    }

    private GraphDatabaseAPI getGraphDatabaseAPI()
    {
        managementService = new TestDatabaseManagementServiceBuilder( directory.storeDir() )
                .setConfig( GraphDatabaseSettings.record_format, getRecordFormatName() )
                .setConfig( "dbms.backup.enabled", "false" ).build();
        GraphDatabaseService database = managementService.database( DEFAULT_DATABASE_NAME );
        return (GraphDatabaseAPI) database;
    }

    protected String getRecordFormatName()
    {
        return StringUtils.EMPTY;
    }

    private static class Sabotage
    {
        private final RelationshipRecord before;
        private final RelationshipRecord after;
        private final RelationshipRecord other;

        Sabotage( RelationshipRecord before, RelationshipRecord after, RelationshipRecord other )
        {
            this.before = before;
            this.after = after;
            this.other = other;
        }

        @Override
        public String toString()
        {
            return "Sabotaged " + before + " --> " + after + ", other relationship " + other;
        }
    }

    private Sabotage sabotage( RelationshipStore store, long id ) throws CloneNotSupportedException
    {
        RelationshipRecord before = store.getRecord( id, store.newRecord(), RecordLoad.NORMAL );
        RelationshipRecord after = before.clone();

        long otherReference;
        if ( !after.isFirstInFirstChain() )
        {
            after.setFirstPrevRel( otherReference = after.getFirstPrevRel() + 1 );
        }
        else
        {
            after.setFirstNextRel( otherReference = after.getFirstNextRel() + 1 );
        }

        store.prepareForCommit( after );
        store.updateRecord( after );

        RelationshipRecord other = store.getRecord( otherReference, store.newRecord(), RecordLoad.FORCE );
        return new Sabotage( before, after, other );
    }
}
