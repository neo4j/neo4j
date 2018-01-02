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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.consistency.statistics.Statistics;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.direct.DirectStoreAccess;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.RandomRule;
import org.neo4j.test.TargetDirectory;
import static org.junit.Assert.assertTrue;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class DetectAllRelationshipInconsistenciesIT
{
    public final TargetDirectory.TestDirectory directory = TargetDirectory.testDirForTest( getClass() );
    public final RandomRule random = new RandomRule();
    @Rule
    public final RuleChain rules = RuleChain.outerRule( random ).around( directory );

    @Test
    public void shouldDetectSabotagedRelationshipWhereEverItIs() throws Exception
    {
        // GIVEN a database which lots of relationships
        GraphDatabaseAPI db = (GraphDatabaseAPI)
                new GraphDatabaseFactory().newEmbeddedDatabase( directory.absolutePath() );
        Sabotage sabotage;
        try
        {
            Node[] nodes = new Node[1_000];
            Relationship[] relationships = new Relationship[10_000];
            try ( Transaction tx = db.beginTx() )
            {
                for ( int i = 0; i < nodes.length; i++ )
                {
                    nodes[i] = db.createNode();
                }
                for ( int i = 0; i < 10_000; i++ )
                {
                    relationships[i] =
                            random.among( nodes ).createRelationshipTo( random.among( nodes ), MyRelTypes.TEST );
                }
                tx.success();
            }

            // WHEN sabotaging a random relationship
            NeoStores neoStores = db.getDependencyResolver().resolveDependency( NeoStores.class );
            RelationshipStore relationshipStore = neoStores.getRelationshipStore();
            Relationship sabotagedRelationships = random.among( relationships );
            sabotage = sabotage( relationshipStore, sabotagedRelationships.getId() );
        }
        finally
        {
            db.shutdown();
        }

        // THEN the checker should find it, where ever it is in the store
        db = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabase( directory.absolutePath() );
        try
        {
            StoreAccess storeAccess = new StoreAccess( db ).initialize();
            DirectStoreAccess directStoreAccess = new DirectStoreAccess( storeAccess,
                    db.getDependencyResolver().resolveDependency( LabelScanStore.class ),
                    db.getDependencyResolver().resolveDependency( SchemaIndexProvider.class ) );

            int threads = random.intBetween( 2, 10 );
            FullCheck checker =
                    new FullCheck( new Config( stringMap( GraphDatabaseSettings.pagecache_memory.name(), "8m" ) ),
                            ProgressMonitorFactory.NONE, Statistics.NONE,
                            threads );
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
            db.shutdown();
        }
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
            return "Sabotabed " + before + " --> " + after + ", other relationship " + other;
        }
    }

    private Sabotage sabotage( RelationshipStore relationshipStore, long id )
    {
        RelationshipRecord before = relationshipStore.getRecord( id );
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

        relationshipStore.updateRecord( after );
        RelationshipRecord other = relationshipStore.forceGetRecord( otherReference );
        return new Sabotage( before, after, other );
    }
}
