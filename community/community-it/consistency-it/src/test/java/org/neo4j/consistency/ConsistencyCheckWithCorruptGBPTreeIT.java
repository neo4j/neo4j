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
import org.eclipse.collections.api.list.primitive.ImmutableLongList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.GBPTreeBootstrapper;
import org.neo4j.index.internal.gbptree.GBPTreeCorruption;
import org.neo4j.index.internal.gbptree.GBPTreeInspection;
import org.neo4j.index.internal.gbptree.GBPTreePointerType;
import org.neo4j.index.internal.gbptree.InspectingVisitor;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileHandle;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.index.schema.SchemaLayouts;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory.createPageCache;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;

@EphemeralPageCacheExtension
@ExtendWith( RandomExtension.class )
class ConsistencyCheckWithCorruptGBPTreeIT
{
    private static final Label label = Label.label( "label" );
    private static final String propKey1 = "key1";

    @Inject
    EphemeralFileSystemAbstraction fs;
    @Inject
    PageCache pageCache;
    @Inject
    TestDirectory testDirectory;
    @Inject
    RandomRule random;

    @Test
    void assertTreeHeightIsAsExpected() throws Exception
    {
        setup( GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10 );
        MutableObject<Integer> heightRef = new MutableObject<>();
        File[] indexFiles = schemaIndexFiles();
        corruptIndexes( true, ( tree, inspection ) -> heightRef.setValue( inspection.getLastLevel() ), indexFiles );

        final int height = heightRef.getValue();
        assertEquals( 2, height, "This test assumes height of index tree is 2 but height for this index was " + height +
                ". This is most easily regulated by changing number of nodes in setup." );
    }

    @Test
    void shouldNotCheckIndexesIfConfiguredNotTo() throws Exception
    {
        setup( GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10 );
        MutableObject<Long> targetNode = new MutableObject<>();
        File[] indexFiles = schemaIndexFiles();
        corruptIndexes( true, ( tree, inspection ) -> {
            targetNode.setValue( inspection.getRootNode() );
            tree.unsafe( GBPTreeCorruption.pageSpecificCorruption( targetNode.getValue(), GBPTreeCorruption.notATreeNode() ) );
        }, indexFiles );

        ConsistencyFlags flags = new ConsistencyFlags( true, false, false, true, true );
        ConsistencyCheckService.Result result = runConsistencyCheck( NullLogProvider.getInstance(), flags );

        assertTrue( result.isSuccessful(), "Expected store to be consistent when not checking indexes." );
    }

    @Test
    void shouldCheckIndexStructureEvenIfNotCheckingIndexes() throws Exception
    {
        setup( GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10 );
        MutableObject<Long> targetNode = new MutableObject<>();
        File[] indexFiles = schemaIndexFiles();
        corruptIndexes( true, ( tree, inspection ) -> {
            targetNode.setValue( inspection.getRootNode() );
            tree.unsafe( GBPTreeCorruption.pageSpecificCorruption( targetNode.getValue(), GBPTreeCorruption.notATreeNode() ) );
        }, indexFiles );

        ConsistencyFlags flags = new ConsistencyFlags( true, false, true, true, true );
        ConsistencyCheckService.Result result = runConsistencyCheck( NullLogProvider.getInstance(), flags );

        assertFalse( result.isSuccessful(), "Expected store to be inconsistent when checking index structure." );
        assertResultContainsMessage( result, "Page: " + targetNode.getValue() + " is not a tree node page" );
    }

    @Test
    void shouldNotCheckIndexStructureIfConfiguredNotToEvenIfCheckingIndexes() throws Exception
    {
        setup( GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10 );
        MutableObject<Long> targetNode = new MutableObject<>();
        File[] indexFiles = schemaIndexFiles();
        corruptIndexes( true, ( tree, inspection ) -> {
            targetNode.setValue( inspection.getRootNode() );
            tree.unsafe( GBPTreeCorruption.addFreelistEntry( 5 ) );
        }, indexFiles );

        ConsistencyFlags flags = new ConsistencyFlags( true, true, false, true, true );
        ConsistencyCheckService.Result result = runConsistencyCheck( NullLogProvider.getInstance(), flags );

        assertTrue( result.isSuccessful(), "Expected store to be consistent when not checking indexes." );
    }

    @Test
    void shouldReportProgress() throws Exception
    {
        setup( GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10 );

        Writer writer = new StringWriter();
        ProgressMonitorFactory factory = ProgressMonitorFactory.textual( writer );
        ConsistencyCheckService.Result result = runConsistencyCheck( NullLogProvider.getInstance(), factory );

        assertTrue( result.isSuccessful(), "Expected new database to be clean." );
        assertTrue( writer.toString().contains( "Index structure consistency check" ) );
    }

    @Test
    void notATreeNode() throws Exception
    {
        setup( GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10 );
        MutableObject<Long> targetNode = new MutableObject<>();
        File[] indexFiles = schemaIndexFiles();
        corruptIndexes( true, ( tree, inspection ) -> {
            targetNode.setValue( inspection.getRootNode() );
            tree.unsafe( GBPTreeCorruption.pageSpecificCorruption( targetNode.getValue(), GBPTreeCorruption.notATreeNode() ) );
        }, indexFiles );

        ConsistencyCheckService.Result result = runConsistencyCheck( NullLogProvider.getInstance() );

        assertFalse( result.isSuccessful(), "Expected store to be considered inconsistent." );
        assertResultContainsMessage( result, "Page: " + targetNode.getValue() + " is not a tree node page." );
    }

    @Test
    void unknownTreeNodeType() throws Exception
    {
        setup( GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10 );
        MutableObject<Long> targetNode = new MutableObject<>();
        File[] indexFiles = schemaIndexFiles();
        corruptIndexes( true, ( tree, inspection ) -> {
            targetNode.setValue( inspection.getRootNode() );
            tree.unsafe( GBPTreeCorruption.pageSpecificCorruption( targetNode.getValue(), GBPTreeCorruption.unknownTreeNodeType() ) );
        }, indexFiles );

        ConsistencyCheckService.Result result = runConsistencyCheck( NullLogProvider.getInstance() );

        assertFalse( result.isSuccessful(), "Expected store to be considered inconsistent." );
        assertResultContainsMessage( result, "Page: " + targetNode.getValue() + " has an unknown tree node type:" );
    }

    @Test
    void siblingsDontPointToEachOther() throws Exception
    {
        setup( GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10 );
        MutableObject<Long> targetNode = new MutableObject<>();
        File[] indexFiles = schemaIndexFiles();
        corruptIndexes( true, ( tree, inspection ) -> {
            targetNode.setValue( inspection.getLeafNodes().get( 0 ) );
            tree.unsafe( GBPTreeCorruption.pageSpecificCorruption( targetNode.getValue(), GBPTreeCorruption.rightSiblingPointToNonExisting() ) );
        }, indexFiles );

        ConsistencyCheckService.Result result = runConsistencyCheck( NullLogProvider.getInstance() );

        assertFalse( result.isSuccessful(), "Expected store to be considered inconsistent." );
        assertResultContainsMessage( result, "Sibling pointers misaligned." );
    }

    @Test
    void rightmostNodeHasRightSibling() throws Exception
    {
        setup( GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10 );
        File[] indexFiles = schemaIndexFiles();
        corruptIndexes( true, ( tree, inspection ) -> {
            final long root = inspection.getRootNode();
            tree.unsafe( GBPTreeCorruption.pageSpecificCorruption( root, GBPTreeCorruption.setPointer( GBPTreePointerType.rightSibling(), 10 ) ) );
        }, indexFiles );

        ConsistencyCheckService.Result result = runConsistencyCheck( NullLogProvider.getInstance() );

        assertFalse( result.isSuccessful(), "Expected store to be considered inconsistent." );
        assertResultContainsMessage( result, "Expected rightmost node to have no right sibling but was 10" );
    }

    @Test
    void pointerToOldVersionOfTreeNode() throws Exception
    {
        setup( GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10 );
        MutableObject<Long> targetNode = new MutableObject<>();
        File[] indexFiles = schemaIndexFiles();
        corruptIndexes( true, ( tree, inspection ) -> {
            targetNode.setValue( inspection.getRootNode() );
            tree.unsafe(
                    GBPTreeCorruption.pageSpecificCorruption( targetNode.getValue(), GBPTreeCorruption.setPointer( GBPTreePointerType.successor(), 6 ) ) );
        }, indexFiles );

        ConsistencyCheckService.Result result = runConsistencyCheck( NullLogProvider.getInstance() );

        assertFalse( result.isSuccessful(), "Expected store to be considered inconsistent." );
        assertResultContainsMessage( result, "We ended up on tree node " + targetNode.getValue() + " which has a newer generation, successor is: 6" );
    }

    @Test
    void pointerHasLowerGenerationThanNode() throws Exception
    {
        setup( GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10 );
        MutableObject<Long> targetNode = new MutableObject<>();
        MutableObject<Long> rightSibling = new MutableObject<>();
        File[] indexFiles = schemaIndexFiles();
        corruptIndexes( true, ( tree, inspection ) -> {
            final ImmutableLongList leafNodes = inspection.getLeafNodes();
            targetNode.setValue( leafNodes.get( 0 ) );
            rightSibling.setValue( leafNodes.get( 1 ) );
            tree.unsafe( GBPTreeCorruption.pageSpecificCorruption( targetNode.getValue(), GBPTreeCorruption.rightSiblingPointerHasTooLowGeneration() ) );
        }, indexFiles );

        ConsistencyCheckService.Result result = runConsistencyCheck( NullLogProvider.getInstance() );

        assertFalse( result.isSuccessful(), "Expected store to be considered inconsistent." );
        assertResultContainsMessage( result,
                String.format( "Pointer (%s) in tree node %d has pointer generation %d, but target node %d has a higher generation %d.",
                        GBPTreePointerType.rightSibling(), targetNode.getValue(), 1, rightSibling.getValue(), 4 ) );
    }

    @Test
    void keysOutOfOrderInNode() throws Exception
    {
        setup( GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10 );
        MutableObject<Long> targetNode = new MutableObject<>();
        File[] indexFiles = schemaIndexFiles();
        corruptIndexes( true, ( tree, inspection ) -> {
            targetNode.setValue( inspection.getLeafNodes().get( 0 ) );
            int keyCount = inspection.getKeyCounts().get( targetNode.getValue() );
            tree.unsafe( GBPTreeCorruption.pageSpecificCorruption( targetNode.getValue(), GBPTreeCorruption.swapKeyOrderLeaf( 0, 1, keyCount ) ) );
        }, indexFiles );

        ConsistencyCheckService.Result result = runConsistencyCheck( NullLogProvider.getInstance() );

        assertFalse( result.isSuccessful(), "Expected store to be considered inconsistent." );
        assertResultContainsMessage( result, String.format( "Keys in tree node %d are out of order.", targetNode.getValue() ) );
    }

    @Test
    void keysLocatedInWrongNode() throws Exception
    {
        setup( GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10 );
        File[] indexFiles = schemaIndexFiles();
        corruptIndexes( true, ( tree, inspection ) -> {
            final long internalNode = inspection.getNodesPerLevel().get( 1 ).get( 0 );
            int keyCount = inspection.getKeyCounts().get( internalNode );
            tree.unsafe( GBPTreeCorruption.pageSpecificCorruption( internalNode, GBPTreeCorruption.swapChildOrder( 0, 1, keyCount ) ) );
        }, indexFiles );

        ConsistencyCheckService.Result result = runConsistencyCheck( NullLogProvider.getInstance() );

        assertFalse( result.isSuccessful(), "Expected store to be considered inconsistent." );
        assertResultContainsMessage( result, "Expected range for this tree node is" );
    }

    @Test
    void unusedPage() throws Exception
    {
        setup( GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10 );
        File[] indexFiles = schemaIndexFiles();
        corruptIndexes( true, ( tree, inspection ) -> {
            final Long internalNode = inspection.getNodesPerLevel().get( 1 ).get( 0 );
            int keyCount = inspection.getKeyCounts().get( internalNode );
            tree.unsafe( GBPTreeCorruption.pageSpecificCorruption( internalNode,GBPTreeCorruption.setKeyCount( keyCount - 1 ) ) );
        }, indexFiles );

        ConsistencyCheckService.Result result = runConsistencyCheck( NullLogProvider.getInstance() );

        assertFalse( result.isSuccessful(), "Expected store to be considered inconsistent." );
        assertResultContainsMessage( result, "Index has a leaked page that will never be reclaimed, pageId=" );
    }

    @Test
    void pageIdExceedLastId() throws Exception
    {
        setup( GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10 );
        File[] indexFiles = schemaIndexFiles();
        corruptIndexes( true, ( tree, inspection ) -> {
            tree.unsafe( GBPTreeCorruption.decrementFreelistWritePos() );
        }, indexFiles );

        ConsistencyCheckService.Result result = runConsistencyCheck( NullLogProvider.getInstance() );

        assertFalse( result.isSuccessful(), "Expected store to be considered inconsistent." );
        assertResultContainsMessage( result, "Index has a leaked page that will never be reclaimed, pageId=" );
    }

    @Test
    void nodeMetaInconsistency() throws Exception
    {
        setup( GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10 );
        File[] indexFiles = schemaIndexFiles();
        corruptIndexes( true, ( tree, inspection ) -> {
            tree.unsafe( GBPTreeCorruption.pageSpecificCorruption( inspection.getRootNode(), GBPTreeCorruption.decrementAllocOffsetInDynamicNode() ) );
        }, indexFiles );

        ConsistencyCheckService.Result result = runConsistencyCheck( NullLogProvider.getInstance() );

        assertFalse( result.isSuccessful(), "Expected store to be considered inconsistent." );
        assertResultContainsMessage( result, "has inconsistent meta data: Meta data for tree node is inconsistent" );
    }

    @Test
    void pageIdSeenMultipleTimes() throws Exception
    {
        setup( GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10 );
        MutableObject<Long> targetNode = new MutableObject<>();
        File[] indexFiles = schemaIndexFiles();
        corruptIndexes( true, ( tree, inspection ) -> {
            targetNode.setValue( inspection.getRootNode() );
            tree.unsafe( GBPTreeCorruption.addFreelistEntry( targetNode.getValue() ) );
        }, indexFiles );

        ConsistencyCheckService.Result result = runConsistencyCheck( NullLogProvider.getInstance() );

        assertFalse( result.isSuccessful(), "Expected store to be considered inconsistent." );
        assertResultContainsMessage( result,
                "Page id seen multiple times, this means either active tree node is present in freelist or pointers in tree create a loop, pageId=" +
                        targetNode.getValue() );
    }

    @Test
    void crashPointer() throws Exception
    {
        setup( GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10 );
        MutableObject<Long> targetNode = new MutableObject<>();
        File[] indexFiles = schemaIndexFiles();
        corruptIndexes( false, ( tree, inspection ) -> {
            targetNode.setValue( inspection.getRootNode() );
            tree.unsafe( GBPTreeCorruption.pageSpecificCorruption( targetNode.getValue(), GBPTreeCorruption.crashed( GBPTreePointerType.rightSibling() ) ) );
        }, indexFiles );

        ConsistencyCheckService.Result result = runConsistencyCheck( NullLogProvider.getInstance() );

        assertFalse( result.isSuccessful(), "Expected store to be considered inconsistent." );
        assertResultContainsMessage( result, "Crashed pointer found in tree node " + targetNode.getValue() );
    }

    @Test
    void brokenPointer() throws Exception
    {
        setup( GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10 );
        MutableObject<Long> targetNode = new MutableObject<>();
        File[] indexFiles = schemaIndexFiles();
        corruptIndexes( true, ( tree, inspection ) -> {
            targetNode.setValue( inspection.getRootNode() );
            tree.unsafe( GBPTreeCorruption.pageSpecificCorruption( targetNode.getValue(), GBPTreeCorruption.broken( GBPTreePointerType.leftSibling() ) ) );
        }, indexFiles );

        ConsistencyCheckService.Result result = runConsistencyCheck( NullLogProvider.getInstance() );

        assertFalse( result.isSuccessful(), "Expected store to be considered inconsistent." );
        assertResultContainsMessage( result, "Broken pointer found in tree node " + targetNode.getValue() );
    }

    @Test
    void unreasonableKeyCount() throws Exception
    {
        setup( GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10 );
        MutableObject<Long> targetNode = new MutableObject<>();
        File[] indexFiles = schemaIndexFiles();
        corruptIndexes( true, ( tree, inspection ) -> {
            targetNode.setValue( inspection.getRootNode() );
            tree.unsafe( GBPTreeCorruption.pageSpecificCorruption( targetNode.getValue(), GBPTreeCorruption.setKeyCount( Integer.MAX_VALUE ) ) );
        }, indexFiles );

        ConsistencyCheckService.Result result = runConsistencyCheck( NullLogProvider.getInstance() );

        assertFalse( result.isSuccessful(), "Expected store to be considered inconsistent." );
        assertResultContainsMessage( result, "Unexpected keyCount on pageId " + targetNode.getValue() + ", keyCount=" + Integer.MAX_VALUE );
    }

    @Test
    void childNodeFoundAmongParentNodes() throws Exception
    {
        setup( GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10 );
        File[] indexFiles = schemaIndexFiles();
        corruptIndexes( true, ( tree, inspection ) -> {
            final long rootNode = inspection.getRootNode();
            tree.unsafe( GBPTreeCorruption.pageSpecificCorruption( rootNode, GBPTreeCorruption.setChild( 0, rootNode ) ) );
        }, indexFiles );

        ConsistencyCheckService.Result result = runConsistencyCheck( NullLogProvider.getInstance() );

        assertFalse( result.isSuccessful(), "Expected store to be considered inconsistent." );
        assertResultContainsMessage( result, "Circular reference, child tree node found among parent nodes. Parents:" );
    }

    @Test
    void exception() throws Exception
    {
        setup( GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10 );
        File[] indexFiles = schemaIndexFiles();
        corruptIndexes( true, ( tree, inspection ) -> {
            final long rootNode = inspection.getRootNode();
            tree.unsafe( GBPTreeCorruption.pageSpecificCorruption( rootNode, GBPTreeCorruption.setHighestReasonableKeyCount() ) );
        }, indexFiles );

        ConsistencyCheckService.Result result = runConsistencyCheck( NullLogProvider.getInstance() );

        assertFalse( result.isSuccessful(), "Expected store to be considered inconsistent." );
        assertResultContainsMessage( result,
                "Caught exception during consistency check: org.neo4j.index.internal.gbptree.TreeInconsistencyException: Some internal problem causing out of" +
                        " bounds: pageId:" );
    }

    @Test
    void shouldIncludeIndexFileInConsistencyReport() throws Exception
    {
        setup( GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10 );
        File[] indexFiles = schemaIndexFiles();
        List<File> corruptedFiles = corruptIndexes( true, ( tree, inspection ) -> {
            final long rootNode = inspection.getRootNode();
            tree.unsafe( GBPTreeCorruption.pageSpecificCorruption( rootNode, GBPTreeCorruption.notATreeNode() ) );
        }, indexFiles );

        ConsistencyCheckService.Result result = runConsistencyCheck( NullLogProvider.getInstance() );

        assertFalse( result.isSuccessful(), "Expected store to be considered inconsistent." );
        assertResultContainsMessage( result, "Index file: " + corruptedFiles.get( 0 ).getAbsolutePath() );
    }

    @Test
    void multipleCorruptions() throws Exception
    {
        setup( GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10 );
        MutableObject<Long> internalNode = new MutableObject<>();
        File[] indexFiles = schemaIndexFiles();
        corruptIndexes( true, ( tree, inspection ) -> {
            long leafNode = inspection.getLeafNodes().get( 0 );
            internalNode.setValue( inspection.getNodesPerLevel().get( 1 ).get( 0 ) );
            final Integer internalNodeKeyCount = inspection.getKeyCounts().get( internalNode.getValue() );
            tree.unsafe( GBPTreeCorruption.pageSpecificCorruption( leafNode, GBPTreeCorruption.rightSiblingPointToNonExisting() ) );
            tree.unsafe( GBPTreeCorruption.pageSpecificCorruption( internalNode.getValue(), GBPTreeCorruption.swapChildOrder( 0, 1, internalNodeKeyCount ) ) );
            tree.unsafe( GBPTreeCorruption.pageSpecificCorruption( internalNode.getValue(), GBPTreeCorruption.broken( GBPTreePointerType.leftSibling() ) ) );
        }, indexFiles );

        ConsistencyCheckService.Result result = runConsistencyCheck( NullLogProvider.getInstance() );
        assertResultContainsMessage( result, "Index inconsistency: Sibling pointers misaligned." );
        assertResultContainsMessage( result, "Index inconsistency: Expected range for this tree node is" );
        assertResultContainsMessage( result,
                "Index inconsistency: Broken pointer found in tree node " + internalNode.getValue() + ", pointerType='left sibling'" );
        assertResultContainsMessage( result,
                "Index inconsistency: Pointer (left sibling) in tree node " );
    }

    @Test
    void corruptionInLabelScanStore() throws Exception
    {
        setup( GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10 );
        MutableObject<Long> rootNode = new MutableObject<>();
        File labelScanStoreFile = labelScanStoreFile();
        corruptIndexes( true, ( tree, inspection ) -> {
            rootNode.setValue( inspection.getRootNode() );
            tree.unsafe( GBPTreeCorruption.pageSpecificCorruption( rootNode.getValue(), GBPTreeCorruption.broken( GBPTreePointerType.leftSibling() ) ) );
        }, labelScanStoreFile );

        ConsistencyCheckService.Result result = runConsistencyCheck( NullLogProvider.getInstance() );
        assertFalse( result.isSuccessful() );
        assertResultContainsMessage( result, "Index inconsistency: Broken pointer found in tree node " + rootNode.getValue() + ", pointerType='left sibling'" );
        assertResultContainsMessage( result, "Number of inconsistent LABEL_SCAN_DOCUMENT records: 1" );
    }

    @Test
    void multipleCorruptionsInFusionIndex() throws Exception
    {
        setup( GraphDatabaseSettings.SchemaIndex.NATIVE30, db ->
                {
                    try ( Transaction tx = db.beginTx() )
                    {
                        // Also make sure we have some numbers
                        for ( int i = 0; i < 1000; i++ )
                        {
                            Node node = tx.createNode( label );
                            node.setProperty( propKey1, i );
                            Node secondNode = tx.createNode( label );
                            secondNode.setProperty( propKey1, LocalDate.ofEpochDay( i ) );
                        }
                        tx.commit();
                    }
                }
        );

        File[] indexFiles = schemaIndexFiles();
        final List<File> files = corruptIndexes( true, ( tree, inspection ) -> {
            long leafNode = inspection.getLeafNodes().get( 1 );
            long internalNode = inspection.getInternalNodes().get( 0 );
            tree.unsafe( GBPTreeCorruption.pageSpecificCorruption( leafNode, GBPTreeCorruption.rightSiblingPointToNonExisting() ) );
            tree.unsafe( GBPTreeCorruption.pageSpecificCorruption( internalNode, GBPTreeCorruption.setChild( 0, internalNode ) ) );
        }, indexFiles );

        ConsistencyCheckService.Result result = runConsistencyCheck( NullLogProvider.getInstance() );
        for ( File file : files )
        {
            assertResultContainsMessage( result,
                    "Index will be excluded from further consistency checks. Index file: " + file.getAbsolutePath() );
        }
    }

    private void assertResultContainsMessage( ConsistencyCheckService.Result result, String expectedMessage ) throws IOException
    {
        final Reader reader = fs.openAsReader( result.reportFile(), Charset.defaultCharset() );
        final BufferedReader bufferedReader = new BufferedReader( reader );
        final List<String> lines = bufferedReader.lines().collect( Collectors.toList() );
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
        assertTrue( reportContainExpectedMessage, errorMessage );
    }

    private ConsistencyCheckService.Result runConsistencyCheck( LogProvider logProvider ) throws ConsistencyCheckIncompleteException
    {
        return runConsistencyCheck( logProvider, ProgressMonitorFactory.NONE );
    }

    private ConsistencyCheckService.Result runConsistencyCheck( LogProvider logProvider, ConsistencyFlags consistencyFlags )
            throws ConsistencyCheckIncompleteException
    {
        return runConsistencyCheck( logProvider, ProgressMonitorFactory.NONE, consistencyFlags );
    }

    private ConsistencyCheckService.Result runConsistencyCheck( LogProvider logProvider, ProgressMonitorFactory progressFactory )
            throws ConsistencyCheckIncompleteException
    {
        return runConsistencyCheck( logProvider, progressFactory, ConsistencyFlags.DEFAULT );
    }

    private ConsistencyCheckService.Result runConsistencyCheck( LogProvider logProvider, ProgressMonitorFactory progressFactory,
            ConsistencyFlags consistencyFlags )
            throws ConsistencyCheckIncompleteException
    {
        ConsistencyCheckService consistencyCheckService = new ConsistencyCheckService();
        DatabaseLayout databaseLayout = testDirectory.databaseLayout();
        Config config = Config.defaults( neo4j_home, testDirectory.directory().toPath().toAbsolutePath() );
        final FileSystemAbstraction fs = testDirectory.getFileSystem();
        return consistencyCheckService.runFullConsistencyCheck( databaseLayout, config, progressFactory, logProvider, fs, false, consistencyFlags );
    }

    private void setup( GraphDatabaseSettings.SchemaIndex schemaIndex )
    {
        setup( schemaIndex, db -> {} );
    }

    private void setup( GraphDatabaseSettings.SchemaIndex schemaIndex, Consumer<GraphDatabaseService> additionalSetup )
    {
        final File homeDir = testDirectory.directory();
        final DatabaseManagementService dbms = new TestDatabaseManagementServiceBuilder( homeDir )
                .setFileSystem( new UncloseableDelegatingFileSystemAbstraction( testDirectory.getFileSystem() ) )
                .setConfig( GraphDatabaseSettings.default_schema_provider, schemaIndex.providerName() )
                .build();
        try
        {
            final GraphDatabaseService db = dbms.database( DEFAULT_DATABASE_NAME );
            createAnIndex( db );
            additionalSetup.accept( db );
        }
        finally
        {
            dbms.shutdown();
        }
    }

    private File labelScanStoreFile()
    {
        final File dataDir = testDirectory.databaseDir();
        return new File( dataDir, DatabaseFile.LABEL_SCAN_STORE.getName() );
    }

    private File[] schemaIndexFiles() throws IOException
    {
        final File databaseDir = testDirectory.databaseDir();
        File indexDir = new File( databaseDir, "schema/index/" );
        return testDirectory.getFileSystem().streamFilesRecursive( indexDir )
                .map( FileHandle::getFile )
                .toArray( File[]::new );
    }

    private List<File> corruptIndexes( boolean readOnly, CorruptionInject corruptionInject, File... targetFiles ) throws Exception
    {
        List<File> treeFiles = new ArrayList<>();
        try ( JobScheduler jobScheduler = createInitialisedScheduler();
              PageCache pageCache = createPageCache( testDirectory.getFileSystem(), jobScheduler ) )
        {
            SchemaLayouts schemaLayouts = new SchemaLayouts();
            GBPTreeBootstrapper bootstrapper = new GBPTreeBootstrapper( pageCache, schemaLayouts, readOnly );
            for ( File file : targetFiles )
            {
                GBPTreeBootstrapper.Bootstrap bootstrap = bootstrapper.bootstrapTree( file, "generic1" );
                if ( bootstrap.isTree() )
                {
                    treeFiles.add( file );
                    try ( GBPTree<?,?> gbpTree = bootstrap.getTree() )
                    {
                        InspectingVisitor<?,?> visitor = gbpTree.visit( new InspectingVisitor<>() );
                        corruptionInject.corrupt( gbpTree, visitor.get() );
                    }
                }
            }
        }
        return treeFiles;
    }

    private void createAnIndex( GraphDatabaseService db )
    {
        String longString = longString();

        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < 60; i++ )
            {
                Node node = tx.createNode( label );
                // Using long string that only differ in the end make sure index tree will be higher which we need to mess up internal pointers
                String value = longString + i;
                node.setProperty( propKey1, value );
            }
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( label ).on( propKey1 ).create();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.HOURS );
            tx.commit();
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
