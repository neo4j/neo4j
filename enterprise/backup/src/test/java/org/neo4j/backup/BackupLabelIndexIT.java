/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.backup;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings.LabelIndex;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.TestLabels;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.neo4j.backup.BackupServer.DEFAULT_PORT;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class BackupLabelIndexIT
{
    private static final TestLabels LABEL = TestLabels.LABEL_ONE;

    private final RandomRule random = new RandomRule();
    private final TestDirectory directory = TestDirectory.testDirectory();
    private final SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( directory ).around( suppressOutput ).around( random );

    private File sourceDir;
    private File backupDir;

    private GraphDatabaseService db;
    private GraphDatabaseService backupDb;

    @Before
    public void setupDirectories()
    {
        sourceDir = directory.absolutePath();
        backupDir = directory.directory( "backup" );
    }

    @After
    public void shutdownDbs()
    {
        if ( backupDb != null )
        {
            backupDb.shutdown();
        }
        if ( db != null )
        {
            db.shutdown();
        }
    }

    @Test
    public void shouldHandleBackingUpFromSourceThatSwitchesLabelIndexProviderAllIndexConfigMatching() throws Exception
    {
        // GIVEN
        LabelIndex[] types = LabelIndex.values();
        Set<Node> expectedNodes = new HashSet<>();
        for ( int i = 0; i < 5; i++ )
        {
            // WHEN
            LabelIndex index = random.among( types );
            db = db( sourceDir, index, true );
            Node node = createNode( db );
            expectedNodes.add( node );

            // THEN
            backupTo( index, backupDir );
            backupDb = db( backupDir, index, false );
            assertNodes( backupDb, expectedNodes );

            shutdownDbs();
        }
    }

    @Test
    public void shouldHandleBackingUpFromSourceThatSwitchesLabelIndexProviderBackupHasDifferentIndex() throws Exception
    {
        // GIVEN
        LabelIndex[] types = LabelIndex.values();
        Set<Node> expectedNodes = new HashSet<>();
        for ( int i = 0; i < 5; i++ )
        {
            // WHEN
            LabelIndex index = random.among( types );
            db = db( sourceDir, index, true );
            Node node = createNode( db );
            expectedNodes.add( node );

            // THEN
            LabelIndex indexForBackup = random.among( types );
            backupTo( indexForBackup, backupDir );
            backupDb = db( backupDir, indexForBackup, false );
            assertNodes( backupDb, expectedNodes );

            shutdownDbs();
        }
    }

    @Test
    public void shouldHandleBackingUpFromSourceThatSwitchesLabelIndexProviderBackupHasDifferentIndexAlsoInternally()
            throws Exception
    {
        // GIVEN
        LabelIndex[] types = LabelIndex.values();
        Set<Node> expectedNodes = new HashSet<>();
        for ( int i = 0; i < 5; i++ )
        {
            // WHEN
            LabelIndex index = random.among( types );
            db = db( sourceDir, index, true );
            Node node = createNode( db );
            expectedNodes.add( node );

            // THEN
            LabelIndex backupIndex = random.among( types );
            backupTo( backupIndex, backupDir );
            LabelIndex backupDbIndex = random.among( types );
            backupDb = db( backupDir, backupDbIndex, false );
            assertNodes( backupDb, expectedNodes );

            shutdownDbs();
        }
    }

    private static void backupTo( LabelIndex index, File toDir )
    {
        Config config = Config.embeddedDefaults( stringMap( GraphDatabaseSettings.label_index.name(), index.name() ) );
        new BackupService( DefaultFileSystemAbstraction::new, NullLogProvider.getInstance(), new Monitors() )
                .doIncrementalBackupOrFallbackToFull( "localhost", DEFAULT_PORT, toDir,
                        ConsistencyCheck.FULL, config, BackupClient.BIG_READ_TIMEOUT, false );
    }

    private static void assertNodes( GraphDatabaseService db, Set<Node> expectedNodes )
    {
        try ( Transaction tx = db.beginTx();
                ResourceIterator<Node> found = db.findNodes( LABEL ) )
        {
            assertEquals( expectedNodes, asSet( found ) );
            tx.success();
        }
    }

    private static Node createNode( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( LABEL );
            tx.success();
            return node;
        }
    }

    private static GraphDatabaseService db( File storeDir, LabelIndex index, boolean backupEnabled )
    {
        GraphDatabaseBuilder builder =
                new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir )
                .setConfig( GraphDatabaseSettings.label_index, index.name() );
        if ( backupEnabled )
        {
            builder.setConfig( OnlineBackupSettings.online_backup_enabled, Settings.TRUE )
                   .setConfig( OnlineBackupSettings.online_backup_server, "localhost:" + DEFAULT_PORT );

        }
        return builder.newGraphDatabase();
    }
}
