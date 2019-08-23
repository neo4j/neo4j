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

import org.apache.commons.lang3.mutable.MutableObject;
import org.bouncycastle.util.Arrays;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
import org.neo4j.index.internal.gbptree.GBPTreePointerType;
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
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.progress.ProgressMonitorFactory.NONE;
import static org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory.createPageCache;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;

public class ConsistencyCheckWithCorruptGBPTreeIT
{
    private PageCacheRule pageCacheRule = new PageCacheRule();
    private TestDirectory testDirectory = TestDirectory.testDirectory();
    private RandomRule random = new RandomRule();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( testDirectory ).around( pageCacheRule ).around( random );

    @Before
    public void setup()
    {
        File dataDir = testDirectory.storeDir();
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( dataDir );
        try
        {
            createAnIndex( db );
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    public void assertTreeHeightIsAsExpected() throws Exception
    {
        MutableObject<Integer> heightRef = new MutableObject<>();
        corruptIndex( ( tree, inspection ) -> heightRef.setValue( inspection.getLastLevel() ) );

        final int height = heightRef.getValue();
        assertEquals( "This test assumes height of index tree is 2 but height for this index was " + height +
                ". This is most easily regulated by changing number of nodes in setup.", 2, height );
    }

    @Test
    public void notATreeNode() throws Exception
    {
        MutableObject<Long> targetNode = new MutableObject<>();
        corruptIndex( ( tree, inspection ) -> {
            targetNode.setValue( inspection.getRootNode() );
            tree.corrupt( GBPTreeCorruption.pageSpecificCorruption( targetNode.getValue(), GBPTreeCorruption.notATreeNode() ) );
        } );

        ConsistencyCheckService.Result result = runConsistencyCheck( NullLogProvider.getInstance() );

        assertFalse( "Expected store to be considered inconsistent.", result.isSuccessful() );
        assertResultContainsMessage( result, "Page: " + targetNode.getValue() + " is not a tree node page." );
    }

    @Test
    public void unknownTreeNodeType() throws Exception
    {
        MutableObject<Long> targetNode = new MutableObject<>();
        corruptIndex( ( tree, inspection ) -> {
            targetNode.setValue( inspection.getRootNode() );
            tree.corrupt( GBPTreeCorruption.pageSpecificCorruption( targetNode.getValue(), GBPTreeCorruption.unknownTreeNodeType() ) );
        } );

        ConsistencyCheckService.Result result = runConsistencyCheck( NullLogProvider.getInstance() );

        assertFalse( "Expected store to be considered inconsistent.", result.isSuccessful() );
        assertResultContainsMessage( result, "Page: " + targetNode.getValue() + " has an unknown tree node type:" );
    }

    @Test
    public void siblingsDontPointToEachOther() throws Exception
    {
        MutableObject<Long> targetNode = new MutableObject<>();
        corruptIndex( ( tree, inspection ) -> {
            targetNode.setValue( inspection.getLeafNodes().get( 0 ) );
            tree.corrupt( GBPTreeCorruption.pageSpecificCorruption( targetNode.getValue(), GBPTreeCorruption.rightSiblingPointToNonExisting() ) );
        } );

        ConsistencyCheckService.Result result = runConsistencyCheck( NullLogProvider.getInstance() );

        assertFalse( "Expected store to be considered inconsistent.", result.isSuccessful() );
        assertResultContainsMessage( result, "Sibling pointers misaligned." );
    }

    @Test
    public void rightmostNodeHasRightSibling() throws Exception
    {
        corruptIndex( ( tree, inspection ) -> {
            final long root = inspection.getRootNode();
            tree.corrupt( GBPTreeCorruption.pageSpecificCorruption( root, GBPTreeCorruption.setPointer( GBPTreePointerType.rightSibling(), 10 ) ) );
        } );

        ConsistencyCheckService.Result result = runConsistencyCheck( NullLogProvider.getInstance() );

        assertFalse( "Expected store to be considered inconsistent.", result.isSuccessful() );
        assertResultContainsMessage( result, "Expected rightmost node to have no right sibling but was 10" );
    }

    @Test
    public void pointerToOldVersionOfTreeNode() throws Exception
    {
        MutableObject<Long> targetNode = new MutableObject<>();
        corruptIndex( ( tree, inspection ) -> {
            targetNode.setValue( inspection.getRootNode() );
            tree.corrupt(
                    GBPTreeCorruption.pageSpecificCorruption( targetNode.getValue(), GBPTreeCorruption.setPointer( GBPTreePointerType.successor(), 6 ) ) );
        } );

        ConsistencyCheckService.Result result = runConsistencyCheck( NullLogProvider.getInstance() );

        assertFalse( "Expected store to be considered inconsistent.", result.isSuccessful() );
        assertResultContainsMessage( result, "We ended up on tree node " + targetNode.getValue() + " which has a newer generation, successor is: 6" );
    }

    @Test
    public void pointerHasLowerGenerationThanNode() throws Exception
    {
        MutableObject<Long> targetNode = new MutableObject<>();
        MutableObject<Long> rightSibling = new MutableObject<>();
        corruptIndex( ( tree, inspection ) -> {
            final List<Long> leafNodes = inspection.getLeafNodes();
            targetNode.setValue( leafNodes.get( 0 ) );
            rightSibling.setValue( leafNodes.get( 1 ) );
            tree.corrupt( GBPTreeCorruption.pageSpecificCorruption( targetNode.getValue(), GBPTreeCorruption.rightSiblingPointerHasTooLowGeneration() ) );
        } );

        ConsistencyCheckService.Result result = runConsistencyCheck( NullLogProvider.getInstance() );

        assertFalse( "Expected store to be considered inconsistent.", result.isSuccessful() );
        assertResultContainsMessage( result,
                String.format( "Pointer (%s) in tree node %d has pointer generation %d, but target node %d has a higher generation %d.",
                        GBPTreePointerType.rightSibling(), targetNode.getValue(), 1, rightSibling.getValue(), 4 ) );
    }

    @Test
    public void keysOutOfOrderInNode() throws Exception
    {
        MutableObject<Long> targetNode = new MutableObject<>();
        corruptIndex( ( tree, inspection ) -> {
            targetNode.setValue( inspection.getLeafNodes().get( 0 ) );
            int keyCount = inspection.getKeyCounts().get( targetNode.getValue() );
            tree.corrupt( GBPTreeCorruption.pageSpecificCorruption( targetNode.getValue(), GBPTreeCorruption.swapKeyOrderLeaf( 0, 1, keyCount ) ) );
        } );

        ConsistencyCheckService.Result result = runConsistencyCheck( NullLogProvider.getInstance() );

        assertFalse( "Expected store to be considered inconsistent.", result.isSuccessful() );
        assertResultContainsMessage( result, String.format( "Keys in tree node %d are out of order.", targetNode.getValue() ) );
    }

    @Test
    public void keysLocatedInWrongNode() throws Exception
    {
        corruptIndex( ( tree, inspection ) -> {
            final Long internalNode = inspection.getNodesPerLevel().get( 1 ).get( 0 );
            int keyCount = inspection.getKeyCounts().get( internalNode );
            tree.corrupt( GBPTreeCorruption.pageSpecificCorruption( internalNode, GBPTreeCorruption.swapChildOrder( 0, 1, keyCount ) ) );
        } );

        ConsistencyCheckService.Result result = runConsistencyCheck( NullLogProvider.getInstance() );

        assertFalse( "Expected store to be considered inconsistent.", result.isSuccessful() );
        assertResultContainsMessage( result, "Expected range for this tree node is" );
    }

    @Test
    public void unusedPage() throws Exception
    {
        corruptIndex( ( tree, inspection ) -> {
            final Long internalNode = inspection.getNodesPerLevel().get( 1 ).get( 0 );
            int keyCount = inspection.getKeyCounts().get( internalNode );
            tree.corrupt( GBPTreeCorruption.pageSpecificCorruption( internalNode,GBPTreeCorruption.setKeyCount( keyCount - 1 ) ) );
        } );

        ConsistencyCheckService.Result result = runConsistencyCheck( NullLogProvider.getInstance() );

        assertFalse( "Expected store to be considered inconsistent.", result.isSuccessful() );
        assertResultContainsMessage( result, "Index has a leaked page that will never be reclaimed, pageId=" );
    }

    @Test
    public void pageIdExceedLastId() throws Exception
    {
        corruptIndex( ( tree, inspection ) -> {
            tree.corrupt( GBPTreeCorruption.decrementFreelistWritePos() );
        } );

        ConsistencyCheckService.Result result = runConsistencyCheck( NullLogProvider.getInstance() );

        assertFalse( "Expected store to be considered inconsistent.", result.isSuccessful() );
        assertResultContainsMessage( result, "Index has a leaked page that will never be reclaimed, pageId=" );
    }

    @Test
    public void nodeMetaInconsistency() throws Exception
    {
        corruptIndex( ( tree, inspection ) -> {
            tree.corrupt( GBPTreeCorruption.pageSpecificCorruption( inspection.getRootNode(), GBPTreeCorruption.decrementAllocOffsetInDynamicNode() ) );
        } );

        ConsistencyCheckService.Result result = runConsistencyCheck( NullLogProvider.getInstance() );

        assertFalse( "Expected store to be considered inconsistent.", result.isSuccessful() );
        assertResultContainsMessage( result, "has inconsistent meta data: Meta data for tree node is inconsistent" );
    }

    @Test
    public void pageIdSeenMultipleTimes() throws Exception
    {
        MutableObject<Long> targetNode = new MutableObject<>();
        corruptIndex( ( tree, inspection ) -> {
            targetNode.setValue( inspection.getRootNode() );
            tree.corrupt( GBPTreeCorruption.addFreelistEntry( targetNode.getValue() ) );
        } );

        ConsistencyCheckService.Result result = runConsistencyCheck( NullLogProvider.getInstance() );

        assertFalse( "Expected store to be considered inconsistent.", result.isSuccessful() );
        assertResultContainsMessage( result,
                "Page id seen multiple times, this means either active tree node is present in freelist or pointers in tree create a loop, pageId=" +
                        targetNode.getValue() );
    }

    @Test
    public void crashPointer() throws Exception
    {
        MutableObject<Long> targetNode = new MutableObject<>();
        corruptIndex( false, ( tree, inspection ) -> {
            targetNode.setValue( inspection.getRootNode() );
            tree.corrupt( GBPTreeCorruption.pageSpecificCorruption( targetNode.getValue(), GBPTreeCorruption.crashed( GBPTreePointerType.rightSibling() ) ) );
        } );

        ConsistencyCheckService.Result result = runConsistencyCheck( NullLogProvider.getInstance() );

        assertFalse( "Expected store to be considered inconsistent.", result.isSuccessful() );
        assertResultContainsMessage( result, "Crashed pointer found in tree node " + targetNode.getValue() );
    }

    @Test
    public void brokenPointer() throws Exception
    {
        MutableObject<Long> targetNode = new MutableObject<>();
        corruptIndex( ( tree, inspection ) -> {
            targetNode.setValue( inspection.getRootNode() );
            tree.corrupt( GBPTreeCorruption.pageSpecificCorruption( targetNode.getValue(), GBPTreeCorruption.broken( GBPTreePointerType.leftSibling() ) ) );
        } );

        ConsistencyCheckService.Result result = runConsistencyCheck( NullLogProvider.getInstance() );

        assertFalse( "Expected store to be considered inconsistent.", result.isSuccessful() );
        assertResultContainsMessage( result, "Broken pointer found in tree node " + targetNode.getValue() );
    }

    @Test
    public void unreasonableKeyCount() throws Exception
    {
        MutableObject<Long> targetNode = new MutableObject<>();
        corruptIndex( ( tree, inspection ) -> {
            targetNode.setValue( inspection.getRootNode() );
            tree.corrupt( GBPTreeCorruption.pageSpecificCorruption( targetNode.getValue(), GBPTreeCorruption.setKeyCount( Integer.MAX_VALUE ) ) );
        } );

        ConsistencyCheckService.Result result = runConsistencyCheck( NullLogProvider.getInstance() );

        assertFalse( "Expected store to be considered inconsistent.", result.isSuccessful() );
        assertResultContainsMessage( result, "Unexpected keyCount on pageId " + targetNode.getValue() + ", keyCount=" + Integer.MAX_VALUE );
    }

    @Test
    public void childNodeFoundAmongParentNodes() throws Exception
    {
        corruptIndex( ( tree, inspection ) -> {
            final long rootNode = inspection.getRootNode();
            tree.corrupt( GBPTreeCorruption.pageSpecificCorruption( rootNode, GBPTreeCorruption.setChild( 0, rootNode ) ) );
        } );

        ConsistencyCheckService.Result result = runConsistencyCheck( NullLogProvider.getInstance() );

        assertFalse( "Expected store to be considered inconsistent.", result.isSuccessful() );
        assertResultContainsMessage( result, "Circular reference, child tree node found among parent nodes. Parents:" );
    }

    @Test
    public void shouldIncludeIndexFileInConsistencyReport() throws Exception
    {
        File indexFile = corruptIndex( ( tree, inspection ) -> {
            final long rootNode = inspection.getRootNode();
            tree.corrupt( GBPTreeCorruption.pageSpecificCorruption( rootNode, GBPTreeCorruption.notATreeNode() ) );
        } );

        ConsistencyCheckService.Result result = runConsistencyCheck( NullLogProvider.getInstance() );

        assertFalse( "Expected store to be considered inconsistent.", result.isSuccessful() );
        assertResultContainsMessage( result, "Index file: " + indexFile.getAbsolutePath() );
    }

    @Test
    public void lotsOfCorruptions() throws Exception
    {
        corruptIndex( ( tree, inspection ) -> {
            long leafNode = inspection.getLeafNodes().get( 0 );
            long internalNode = inspection.getNodesPerLevel().get( 1 ).get( 1 );
            final Integer internalNodeKeyCount = inspection.getKeyCounts().get( internalNode );
            tree.corrupt( GBPTreeCorruption.pageSpecificCorruption( leafNode, GBPTreeCorruption.rightSiblingPointToNonExisting() ) );
            tree.corrupt( GBPTreeCorruption.pageSpecificCorruption( internalNode, GBPTreeCorruption.swapChildOrder( 0, 1, internalNodeKeyCount ) ) );
            tree.corrupt( GBPTreeCorruption.pageSpecificCorruption( internalNode, GBPTreeCorruption.broken( GBPTreePointerType.leftSibling() ) ) );
        } );

        ConsistencyCheckService.Result result = runConsistencyCheck( NullLogProvider.getInstance() );
        Files.lines( result.reportFile().toPath() ).forEach( System.out::println );
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

    private ConsistencyCheckService.Result runConsistencyCheck( LogProvider logProvider ) throws ConsistencyCheckIncompleteException
    {
        ConsistencyCheckService consistencyCheckService = new ConsistencyCheckService();
        DatabaseLayout databaseLayout = DatabaseLayout.of( testDirectory.storeDir() );
        Config config = Config.defaults();
        ProgressMonitorFactory progressFactory = NONE;
        return consistencyCheckService.runFullConsistencyCheck( databaseLayout, config, progressFactory, logProvider, false );
    }

    private File corruptIndex( CorruptionInject corruptionInject ) throws Exception
    {
        return corruptIndex( true, corruptionInject );
    }

    private File corruptIndex( boolean readOnly, CorruptionInject corruptionInject ) throws Exception
    {
        FileSystemAbstraction fs = testDirectory.getFileSystem();
        File indexDir = new File( testDirectory.storeDir(), "schema/index/" );
        File indexFile = fs.streamFilesRecursive( indexDir )
                .map( FileHandle::getFile )
                .findFirst()
                .orElseThrow( () -> new IllegalStateException( "Test need at least one index file" ) );
        try ( JobScheduler jobScheduler = createInitialisedScheduler();
              PageCache pageCache = createPageCache( fs, jobScheduler ) )
        {
            SchemaLayouts schemaLayouts = new SchemaLayouts();
            GBPTreeBootstrapper bootstrapper = new GBPTreeBootstrapper( pageCache, schemaLayouts, readOnly );
            GBPTreeBootstrapper.Bootstrap bootstrap = bootstrapper.bootstrapTree( indexFile, "generic1" );
            try ( GBPTree<?,?> gbpTree = bootstrap.tree )
            {
                GBPTreeInspection<?,?> inspection = gbpTree.inspect();
                corruptionInject.corrupt( gbpTree, inspection );
            }
        }
        return indexFile;
    }

    private void createAnIndex( GraphDatabaseService db )
    {
        Label label = Label.label( "label" );
        String propKey1 = "key1";
        String longString = longString();

        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < 40; i++ )
            {
                Node firstNode = db.createNode( label );
                // Using long string that only differ in the end make sure index tree will be higher which we need to mess up internal pointers
                String value = longString + i;
                firstNode.setProperty( propKey1, value );
            }
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

    private String longString()
    {
        char[] chars = new char[1000];
        Arrays.fill( chars, 'a' );
        return new String( chars );
    }

    private interface CorruptionInject
    {
        void corrupt( GBPTree<?,?> tree, GBPTreeInspection<?,?> inspection ) throws IOException;
    }
}
