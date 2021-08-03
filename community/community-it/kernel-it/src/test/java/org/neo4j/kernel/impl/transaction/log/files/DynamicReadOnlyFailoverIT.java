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
package org.neo4j.kernel.impl.transaction.log.files;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.io.ByteUnit;
import org.neo4j.kernel.api.exceptions.ReadOnlyDbException;
import org.neo4j.kernel.impl.transaction.log.rotation.monitor.LogRotationMonitorAdapter;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.utils.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.dynamic_read_only_failover;
import static org.neo4j.configuration.GraphDatabaseSettings.logical_log_rotation_threshold;

@DbmsExtension
@Isolated
@EnabledOnOs( OS.LINUX )
@Disabled
class DynamicReadOnlyFailoverIT
{
    private static final String TEST_SCOPE = "preallocation test";
    private static final int NUMBER_OF_NODES = 100;
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private GraphDatabaseAPI database;
    @Inject
    private Config config;
    @Inject
    private Monitors monitors;

    @Test
    void switchDatabaseToReadOnlyModeOnPreallocationFailure()
    {
        long initialRotationThreshold = ByteUnit.kibiBytes( 128 );
        Label marker = Label.label( "marker" );
        try ( Transaction transaction = database.beginTx() )
        {
            for ( int i = 0; i < NUMBER_OF_NODES; i++ )
            {
                transaction.createNode( marker );
            }
            transaction.commit();
        }

        config.setDynamic( logical_log_rotation_threshold, initialRotationThreshold, TEST_SCOPE );
        monitors.addMonitorListener( new LogRotationMonitorAdapter()
        {
            @Override
            public void startRotation( long currentLogVersion )
            {
                config.setDynamic( logical_log_rotation_threshold, getUnavailableBytes(), TEST_SCOPE );
                super.startRotation( currentLogVersion );
            }
        } );

        var e = assertThrows( Exception.class, () ->
        {
            try ( Transaction transaction = database.beginTx() )
            {
                Node node = transaction.createNode();
                node.setProperty( "a", RandomStringUtils.randomAscii( (int) (initialRotationThreshold + 100) ) );
                transaction.commit();
            }
        } );

        assertThat( e ).hasRootCauseInstanceOf( ReadOnlyDbException.class ).hasRootCauseMessage(
                "This Neo4j instance is read only for the database " + database.databaseName() );
        assertDoesNotThrow( () ->
        {
            try ( Transaction transaction = database.beginTx() )
            {
                assertEquals( NUMBER_OF_NODES, Iterators.count( transaction.findNodes( marker ) ) );
            }
        } );

        var writeException = assertThrows( Exception.class, () ->
        {
            try ( Transaction transaction = database.beginTx() )
            {
                transaction.createNode();
                transaction.commit();
            }
        } );
        assertThat( writeException ).hasMessageContaining( "read only" );
    }

    @Test
    void doNotSwitchDatabaseToReadOnlyModeWhenFailoverIsDisabled()
    {
        long initialRotationThreshold = ByteUnit.kibiBytes( 128 );
        Label marker = Label.label( "marker" );
        try ( Transaction transaction = database.beginTx() )
        {
            for ( int i = 0; i < NUMBER_OF_NODES; i++ )
            {
                transaction.createNode( marker );
            }
            transaction.commit();
        }

        config.setDynamic( dynamic_read_only_failover, false, TEST_SCOPE );
        config.setDynamic( logical_log_rotation_threshold, initialRotationThreshold, TEST_SCOPE );
        monitors.addMonitorListener( new LogRotationMonitorAdapter()
        {
            @Override
            public void startRotation( long currentLogVersion )
            {
                config.setDynamic( logical_log_rotation_threshold, getUnavailableBytes(), TEST_SCOPE );
                super.startRotation( currentLogVersion );
            }
        } );

        assertDoesNotThrow( () ->
        {
            try ( Transaction transaction = database.beginTx() )
            {
                Node node = transaction.createNode();
                node.setProperty( "a", RandomStringUtils.randomAscii( (int) (initialRotationThreshold + 100) ) );
                transaction.commit();
            }
        } );

        assertDoesNotThrow( () ->
        {
            try ( Transaction transaction = database.beginTx() )
            {
                assertEquals( NUMBER_OF_NODES, Iterators.count( transaction.findNodes( marker ) ) );
            }
        } );

        assertDoesNotThrow( () ->
        {
            try ( Transaction transaction = database.beginTx() )
            {
                transaction.createNode();
                transaction.commit();
            }
        } );
    }

    private long getUnavailableBytes()
    {
        try
        {
            return Files.getFileStore( testDirectory.homePath() ).getUsableSpace() + ByteUnit.gibiBytes( 10 );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
