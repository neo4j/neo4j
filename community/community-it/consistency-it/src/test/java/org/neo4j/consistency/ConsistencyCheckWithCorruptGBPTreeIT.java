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
package org.neo4j.consistency;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.GBPTreeBootstrapper;
import org.neo4j.index.internal.gbptree.GBPTreeCorruption;
import org.neo4j.index.internal.gbptree.GBPTreeInspection;
import org.neo4j.io.fs.FileHandle;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.index.schema.SchemaLayouts;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;

import static org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory.createPageCache;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;

public class ConsistencyCheckWithCorruptGBPTreeIT
{
    private PageCacheRule pageCacheRule = new PageCacheRule();
    private TestDirectory testDirectory = TestDirectory.testDirectory();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( testDirectory ).around( pageCacheRule );

    @Test
    public void shouldReportCorruptionInGBPTreeAndExcludeIndexFromConsistencyCheck() throws Exception
    {
        File dataDir = testDirectory.storeDir();
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( dataDir );
        try
        {
            createSomeIndexes( db );
        }
        finally
        {
            db.shutdown();
        }

        corruptIndexes( dataDir );

        runConsistencyCheck( dataDir );
    }

    private void runConsistencyCheck( File dataDir ) throws ConsistencyCheckIncompleteException
    {
        ConsistencyCheckService consistencyCheckService = new ConsistencyCheckService();
        DatabaseLayout databaseLayout = DatabaseLayout.of( dataDir );
        Config config = Config.defaults();
        ProgressMonitorFactory progressFactory = ProgressMonitorFactory.NONE;
        NullLogProvider logProvider = NullLogProvider.getInstance();
        consistencyCheckService.runFullConsistencyCheck( databaseLayout, config, progressFactory, logProvider, false );
    }

    private void corruptIndexes( File dataDir ) throws Exception
    {
        FileSystemAbstraction fs = testDirectory.getFileSystem();
        try ( JobScheduler jobScheduler = createInitialisedScheduler();
              PageCache pageCache = createPageCache( fs, jobScheduler ) )
        {
            File indexDir = new File( dataDir, "schema/index/" );
            List<File> indexFiles = fs.streamFilesRecursive( indexDir )
                    .map( FileHandle::getFile )
                    .collect( Collectors.toList() );

            SchemaLayouts schemaLayouts = new SchemaLayouts();
            GBPTreeBootstrapper bootstrapper = new GBPTreeBootstrapper( pageCache, schemaLayouts, false );
            for ( File indexFile : indexFiles )
            {
                GBPTreeBootstrapper.Bootstrap bootstrap = bootstrapper.bootstrapTree( indexFile, "generic1" );
                try ( GBPTree<?,?> gbpTree = bootstrap.tree )
                {
                    GBPTreeInspection<?,?> inspection = gbpTree.inspect();
                    gbpTree.corrupt( GBPTreeCorruption.pageSpecificCorruption( inspection.getRootNode(), GBPTreeCorruption.notATreeNode() ) );
                }
            }
        }
    }

    private void createSomeIndexes( GraphDatabaseService db )
    {
        Label label = Label.label( "label" );
        RelationshipType relType = RelationshipType.withName( "TYPE" );
        String propKey1 = "key1";
        String propKey2 = "key2";
        String propKey3 = "key3";

        try ( Transaction tx = db.beginTx() )
        {
            Node firstNode = db.createNode( label );
            firstNode.setProperty( propKey1, "hej" );
            firstNode.setProperty( propKey2, "hå" );
            firstNode.setProperty( propKey3, "jobba på" );
            Node secondNode = db.createNode( label );
            secondNode.setProperty( propKey1, "hej" );
            secondNode.setProperty( propKey2, "då" );
            secondNode.setProperty( propKey3, "gå och gå" );
            firstNode.createRelationshipTo( secondNode, relType );
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( label ).on( propKey1 ).create();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( label ).assertPropertyIsUnique( propKey3 ).create();
            tx.success();
        }
    }
}
