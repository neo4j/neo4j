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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
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
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.progress.ProgressMonitorFactory.NONE;
import static org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory.createPageCache;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;

public class ConsistencyCheckWithCorruptGBPTreeIT
{
    private PageCacheRule pageCacheRule = new PageCacheRule();
    private TestDirectory testDirectory = TestDirectory.testDirectory();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( testDirectory ).around( pageCacheRule );

    @Before
    public void setup()
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
    }

    @Test
    public void shouldReportCorruptionInGBPTreeAndExcludeIndexFromRestOfConsistencyCheck() throws Exception
    {
        final File dataDir = testDirectory.storeDir();
        corruptIndexes( dataDir, ( tree, inspection ) -> {
            long rootNode = inspection.getRootNode();
            tree.corrupt( GBPTreeCorruption.pageSpecificCorruption( rootNode, GBPTreeCorruption.notATreeNode() ) );
        } );

        ConsistencyCheckService.Result result = runConsistencyCheck( dataDir, NullLogProvider.getInstance() );

        assertFalse( "Expected store to be considered inconsistent.", result.isSuccessful() );
        assertResultContainsMessage( result, "Page: 5 is not a tree node page." );
    }

    @Test
    public void shouldIncludeIndexFileInConsistencyReport() throws Exception
    {
        File dataDir = testDirectory.storeDir();
        List<File> indexFile = corruptIndexes( dataDir, ( tree, inspection ) -> {
            long rootNode = inspection.getRootNode();
            tree.corrupt( GBPTreeCorruption.pageSpecificCorruption( rootNode, GBPTreeCorruption.notATreeNode() ) );
        } );

        ConsistencyCheckService.Result result = runConsistencyCheck( dataDir, NullLogProvider.getInstance() );

        Files.lines( result.reportFile().toPath() ).forEach( System.out::println );
        assertFalse( "Expected store to be considered inconsistent.", result.isSuccessful() );
        for ( File file : indexFile )
        {
            assertResultContainsMessage( result, "Index file: " + file.getAbsolutePath() );
        }
    }

    private void assertResultContainsMessage( ConsistencyCheckService.Result result, String expectedMessage ) throws IOException
    {
        final List<String> lines = Files.readAllLines( result.reportFile().toPath() );
        boolean reportContainExpectedMessage = false;
        for ( String line : lines )
        {
            if ( line.contains( expectedMessage ) )
            {
                reportContainExpectedMessage = true;
                break;
            }
        }
        String errorMessage = format("Expected consistency report to contain message `%s'. Real result was: %s%n",
                expectedMessage, String.join( System.lineSeparator(), lines ) );
        assertTrue( errorMessage, reportContainExpectedMessage );
    }

    private ConsistencyCheckService.Result runConsistencyCheck( File dataDir, LogProvider logProvider ) throws ConsistencyCheckIncompleteException
    {
        ConsistencyCheckService consistencyCheckService = new ConsistencyCheckService();
        DatabaseLayout databaseLayout = DatabaseLayout.of( dataDir );
        Config config = Config.defaults();
        ProgressMonitorFactory progressFactory = NONE;
        return consistencyCheckService.runFullConsistencyCheck( databaseLayout, config, progressFactory, logProvider, false );
    }

    private List<File> corruptIndexes( File dataDir, CorruptionInject corruptionInject ) throws Exception
    {
        FileSystemAbstraction fs = testDirectory.getFileSystem();
        File indexDir = new File( dataDir, "schema/index/" );
        List<File> indexFiles = fs.streamFilesRecursive( indexDir )
                .map( FileHandle::getFile )
                .collect( Collectors.toList() );
        try ( JobScheduler jobScheduler = createInitialisedScheduler();
              PageCache pageCache = createPageCache( fs, jobScheduler ) )
        {
            SchemaLayouts schemaLayouts = new SchemaLayouts();
            GBPTreeBootstrapper bootstrapper = new GBPTreeBootstrapper( pageCache, schemaLayouts, false );
            for ( File indexFile : indexFiles )
            {
                GBPTreeBootstrapper.Bootstrap bootstrap = bootstrapper.bootstrapTree( indexFile, "generic1" );
                try ( GBPTree<?,?> gbpTree = bootstrap.tree )
                {
                    GBPTreeInspection<?,?> inspection = gbpTree.inspect();
                    corruptionInject.corrupt( gbpTree, inspection );
                }
            }
        }
        return indexFiles;
    }

    private void createSomeIndexes( GraphDatabaseService db )
    {
        Label label = Label.label( "label" );
        String propKey1 = "key1";

        try ( Transaction tx = db.beginTx() )
        {
            Node firstNode = db.createNode( label );
            firstNode.setProperty( propKey1, "hej" );
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( label ).on( propKey1 ).create();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.HOURS );
            tx.success();
        }
    }

    private interface CorruptionInject
    {
        void corrupt( GBPTree<?,?> tree, GBPTreeInspection<?,?> inspection ) throws IOException;
    }
}
