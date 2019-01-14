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
package org.neo4j.consistency.checking;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.index.labelscan.NativeLabelScanStore;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;
import org.neo4j.test.rule.RandomRule;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.progress.ProgressMonitorFactory.NONE;
import static org.neo4j.io.fs.FileUtils.copyFile;
import static org.neo4j.test.TestLabels.LABEL_ONE;
import static org.neo4j.test.TestLabels.LABEL_THREE;
import static org.neo4j.test.TestLabels.LABEL_TWO;

public class AllNodesInStoreExistInLabelIndexTest
{
    @Rule
    public final DatabaseRule db = new EmbeddedDatabaseRule();

    @Rule
    public final RandomRule random = new RandomRule();

    private final AssertableLogProvider log = new AssertableLogProvider();
    private static final Label[] LABEL_ALPHABET = new Label[]{LABEL_ONE, LABEL_TWO, LABEL_THREE};
    private static final Label EXTRA_LABEL = Label.label( "extra" );
    private static final double DELETE_RATIO = 0.2;
    private static final double UPDATE_RATIO = 0.2;
    private static final int NODE_COUNT_BASELINE = 10;

    @Test
    public void mustReportSuccessfulForConsistentLabelScanStore() throws Exception
    {
        // given
        someData();
        db.shutdownAndKeepStore();

        // when
        ConsistencyCheckService.Result result = fullConsistencyCheck();

        // then
        assertTrue( "Expected consistency check to succeed", result.isSuccessful() );
    }

    @Test
    public void reportNotCleanLabelIndex() throws IOException, ConsistencyCheckIncompleteException
    {
        File storeDir = db.getStoreDir();
        someData();
        db.resolveDependency( CheckPointer.class ).forceCheckPoint( new SimpleTriggerInfo( "forcedCheckpoint" ) );
        File labelIndexFileCopy = new File( storeDir, "label_index_copy" );
        copyFile( new File( storeDir, NativeLabelScanStore.FILE_NAME ), labelIndexFileCopy );

        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( LABEL_ONE );
            tx.success();
        }

        db.shutdownAndKeepStore();

        copyFile( labelIndexFileCopy, new File( storeDir, NativeLabelScanStore.FILE_NAME ) );

        ConsistencyCheckService.Result result = fullConsistencyCheck();
        assertFalse( "Expected consistency check to fail", result.isSuccessful() );
        assertThat( readReport( result ),
                hasItem( containsString("WARN : Label index was not properly shutdown and rebuild is required.") ) );
    }

    @Test
    public void reportNotCleanLabelIndexWithCorrectData() throws IOException, ConsistencyCheckIncompleteException
    {
        File storeDir = db.getStoreDir();
        someData();
        db.resolveDependency( CheckPointer.class ).forceCheckPoint( new SimpleTriggerInfo( "forcedCheckpoint" ) );
        File labelIndexFileCopy = new File( storeDir, "label_index_copy" );
        copyFile( new File( storeDir, NativeLabelScanStore.FILE_NAME ), labelIndexFileCopy );

        db.shutdownAndKeepStore();

        copyFile( labelIndexFileCopy, new File( storeDir, NativeLabelScanStore.FILE_NAME ) );

        ConsistencyCheckService.Result result = fullConsistencyCheck();
        assertTrue( "Expected consistency check to fail", result.isSuccessful() );
        assertThat( readReport( result ),
                hasItem( containsString("WARN : Label index was not properly shutdown and rebuild is required.") ) );
    }

    @Test
    public void mustReportMissingNode() throws Exception
    {
        // given
        someData();
        File labelIndexFileCopy = copyLabelIndexFile();

        // when
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( LABEL_ONE );
            tx.success();
        }

        // and
        replaceLabelIndexWithCopy( labelIndexFileCopy );
        db.shutdownAndKeepStore();

        // then
        ConsistencyCheckService.Result result = fullConsistencyCheck();
        assertFalse( "Expected consistency check to fail", result.isSuccessful() );
    }

    @Test
    public void mustReportMissingLabel() throws Exception
    {
        // given
        List<Pair<Long,Label[]>> nodesInStore = someData();
        File labelIndexFileCopy = copyLabelIndexFile();

        // when
        try ( Transaction tx = db.beginTx() )
        {
            addLabelToExistingNode( nodesInStore );
            tx.success();
        }

        // and
        replaceLabelIndexWithCopy( labelIndexFileCopy );
        db.shutdownAndKeepStore();

        // then
        ConsistencyCheckService.Result result = fullConsistencyCheck();
        assertFalse( "Expected consistency check to fail", result.isSuccessful() );
    }

    @Test
    public void mustReportExtraLabelsOnExistingNode() throws Exception
    {
        // given
        List<Pair<Long,Label[]>> nodesInStore = someData();
        File labelIndexFileCopy = copyLabelIndexFile();

        // when
        try ( Transaction tx = db.beginTx() )
        {
            removeLabelFromExistingNode( nodesInStore );
            tx.success();
        }

        // and
        replaceLabelIndexWithCopy( labelIndexFileCopy );
        db.shutdownAndKeepStore();

        // then
        ConsistencyCheckService.Result result = fullConsistencyCheck();
        assertFalse( "Expected consistency check to fail", result.isSuccessful() );
    }

    @Test
    public void mustReportExtraNode() throws Exception
    {
        // given
        List<Pair<Long,Label[]>> nodesInStore = someData();
        File labelIndexFileCopy = copyLabelIndexFile();

        // when
        try ( Transaction tx = db.beginTx() )
        {
            removeExistingNode( nodesInStore );
            tx.success();
        }

        // and
        replaceLabelIndexWithCopy( labelIndexFileCopy );
        db.shutdownAndKeepStore();

        // then
        ConsistencyCheckService.Result result = fullConsistencyCheck();
        assertFalse( "Expected consistency check to fail", result.isSuccessful() );
    }

    private List<String> readReport( ConsistencyCheckService.Result result )
            throws IOException
    {
        return Files.readAllLines( result.reportFile().toPath() );
    }

    private void removeExistingNode( List<Pair<Long,Label[]>> nodesInStore )
    {
        Node node;
        Label[] labels;
        do
        {
            int targetIndex = random.nextInt( nodesInStore.size() );
            Pair<Long,Label[]> existingNode = nodesInStore.get( targetIndex );
            node = db.getNodeById( existingNode.first() );
            labels = existingNode.other();
        }
        while ( labels.length == 0 );
        node.delete();
    }

    private void addLabelToExistingNode( List<Pair<Long,Label[]>> nodesInStore )
    {
        int targetIndex = random.nextInt( nodesInStore.size() );
        Pair<Long,Label[]> existingNode = nodesInStore.get( targetIndex );
        Node node = db.getNodeById( existingNode.first() );
        node.addLabel( EXTRA_LABEL );
    }

    private void removeLabelFromExistingNode( List<Pair<Long,Label[]>> nodesInStore )
    {
        Pair<Long,Label[]> existingNode;
        Node node;
        do
        {
            int targetIndex = random.nextInt( nodesInStore.size() );
            existingNode = nodesInStore.get( targetIndex );
            node = db.getNodeById( existingNode.first() );
        }
        while ( existingNode.other().length == 0 );
        node.removeLabel( existingNode.other()[0] );
    }

    private void replaceLabelIndexWithCopy( File labelIndexFileCopy ) throws IOException
    {
        db.restartDatabase( ( fs, directory ) ->
        {
            File storeDir = db.getStoreDir();
            fs.deleteFile( new File( storeDir, NativeLabelScanStore.FILE_NAME ) );
            fs.copyFile( labelIndexFileCopy, new File( storeDir, NativeLabelScanStore.FILE_NAME ) );
        } );
    }

    private File copyLabelIndexFile() throws IOException
    {
        File storeDir = db.getStoreDir();
        File labelIndexFileCopy = new File( storeDir, "label_index_copy" );
        db.restartDatabase( ( fs, directory ) ->
                fs.copyFile( new File( storeDir, NativeLabelScanStore.FILE_NAME ), labelIndexFileCopy ) );
        return labelIndexFileCopy;
    }

    List<Pair<Long,Label[]>> someData()
    {
        return someData( 50 );
    }

    List<Pair<Long,Label[]>> someData( int numberOfModifications )
    {
        List<Pair<Long,Label[]>> existingNodes;
        existingNodes = new ArrayList<>();
        try ( Transaction tx = db.beginTx() )
        {
            randomModifications( existingNodes, numberOfModifications );
            tx.success();
        }
        return existingNodes;
    }

    private List<Pair<Long,Label[]>> randomModifications( List<Pair<Long,Label[]>> existingNodes,
            int numberOfModifications )
    {
        for ( int i = 0; i < numberOfModifications; i++ )
        {
            double selectModification = random.nextDouble();
            if ( existingNodes.size() < NODE_COUNT_BASELINE || selectModification >= DELETE_RATIO + UPDATE_RATIO )
            {
                createNewNode( existingNodes );
            }
            else if ( selectModification < DELETE_RATIO )
            {
                deleteExistingNode( existingNodes );
            }
            else
            {
                modifyLabelsOnExistingNode( existingNodes );
            }
        }
        return existingNodes;
    }

    private void createNewNode( List<Pair<Long,Label[]>> existingNodes )
    {
        Label[] labels = randomLabels();
        Node node = db.createNode( labels );
        existingNodes.add( Pair.of( node.getId(), labels ) );
    }

    private void modifyLabelsOnExistingNode( List<Pair<Long,Label[]>> existingNodes )
    {
        int targetIndex = random.nextInt( existingNodes.size() );
        Pair<Long,Label[]> existingPair = existingNodes.get( targetIndex );
        long nodeId = existingPair.first();
        Node node = db.getNodeById( nodeId );
        node.getLabels().forEach( node::removeLabel );
        Label[] newLabels = randomLabels();
        for ( Label label : newLabels )
        {
            node.addLabel( label );
        }
        existingNodes.remove( targetIndex );
        existingNodes.add( Pair.of( nodeId, newLabels ) );
    }

    private void deleteExistingNode( List<Pair<Long,Label[]>> existingNodes )
    {
        int targetIndex = random.nextInt( existingNodes.size() );
        Pair<Long,Label[]> existingPair = existingNodes.get( targetIndex );
        Node node = db.getNodeById( existingPair.first() );
        node.delete();
        existingNodes.remove( targetIndex );
    }

    private Label[] randomLabels()
    {
        List<Label> labels = new ArrayList<>( 3 );
        for ( Label label : LABEL_ALPHABET )
        {
            if ( random.nextBoolean() )
            {
                labels.add( label );
            }
        }
        return labels.toArray( new Label[labels.size()] );
    }

    ConsistencyCheckService.Result fullConsistencyCheck()
            throws ConsistencyCheckIncompleteException, IOException
    {
        try ( FileSystemAbstraction fsa = new DefaultFileSystemAbstraction() )
        {
            ConsistencyCheckService service = new ConsistencyCheckService();
            Config config = Config.defaults();
            return service.runFullConsistencyCheck( db.getStoreDir(), config, NONE, log, fsa, true,
                    new ConsistencyFlags( config ) );
        }
    }
}
