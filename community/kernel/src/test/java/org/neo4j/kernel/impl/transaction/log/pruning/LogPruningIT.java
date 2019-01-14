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
package org.neo4j.kernel.impl.transaction.log.pruning;

import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.keep_logical_logs;

public class LogPruningIT
{
    @Rule
    public final DatabaseRule db = new EmbeddedDatabaseRule().withSetting( keep_logical_logs, "true" );

    private final SimpleTriggerInfo triggerInfo = new SimpleTriggerInfo( "forced trigger" );

    @Test
    public void pruningStrategyShouldBeDynamic() throws IOException
    {
        CheckPointer checkPointer = getInstanceFromDb( CheckPointer.class );
        Config config = getInstanceFromDb( Config.class );
        FileSystemAbstraction fs = getInstanceFromDb( FileSystemAbstraction.class );

        LogFiles logFiles = LogFilesBuilder.builder( db.getStoreDir(), fs )
                .withLogVersionRepository( new SimpleLogVersionRepository() )
                .withLastCommittedTransactionIdSupplier( () -> 1 )
                .withTransactionIdStore( new SimpleTransactionIdStore() ).build();

        // Force transaction log rotation
        writeTransactionsAndRotateTwice();

        // Checkpoint to make sure strategy is evaluated
        checkPointer.forceCheckPoint( triggerInfo );

        // Make sure file is still there since we have disable pruning
        assertThat( countTransactionLogs( logFiles ), is( 3 ) );

        // Change pruning to true
        config.updateDynamicSetting( keep_logical_logs.name(), "false", "test" );

        // Checkpoint to make sure strategy is evaluated
        checkPointer.forceCheckPoint( triggerInfo );

        // Make sure file is removed
        assertThat( countTransactionLogs( logFiles ), is( 2 ) );
    }

    private void writeTransactionsAndRotateTwice() throws IOException
    {
        LogRotation logRotation = db.getDependencyResolver().resolveDependency( LogRotation.class );
        // Apparently we always keep an extra log file what even though the threshold is reached... produce two then
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }
        logRotation.rotateLogFile();
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }
        logRotation.rotateLogFile();
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }
    }

    private <T> T getInstanceFromDb( Class<T> clazz )
    {
        return db.getDependencyResolver().resolveDependency( clazz );
    }

    private int countTransactionLogs( LogFiles logFiles )
    {
        return logFiles.logFiles().length;
    }
}
