/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.backup;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static org.hamcrest.Matchers.any;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class OnlineBackupCommandConfigLoaderTest
{
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    @Rule
    public ExpectedException expected = ExpectedException.none();

    OnlineBackupCommandConfigLoader subject;

    Path configDir;
    Path homeDir;

    @Before
    public void setup() throws IOException
    {
        configDir = testDirectory.directory( "config" ).toPath();
        setupConfigInDirectory( configDir );
        homeDir = testDirectory.directory( "home" ).toPath();
        setupConfigInDirectory( homeDir );
        subject = new OnlineBackupCommandConfigLoader( homeDir, configDir );
    }

    private void setupConfigInDirectory( Path configDir ) throws IOException
    {
        String anyValidSetting = format( "dbms.backup.address = %s\n", "localhost:1234");
        File configFile = configDir.resolve( "neo4j.conf" ).toFile();
        appendToFile( configFile, anyValidSetting );
    }

    @Test
    public void errorHandledForNonExistingAdditionalConfigFile() throws CommandFailed, IOException
    {
        // given
        File additionalConf = testDirectory.file( "neo4j.conf" );
        additionalConf.delete();

        // and
        expected.expect( CommandFailed.class );
        expected.expectCause( any( FileNotFoundException.class ) );

        // expect
        assertFalse( additionalConf.exists() );
        subject.loadConfig( Optional.of( additionalConf.toPath() ) );
    }

    @Test
    public void prioritiseConfigDirOverHomeDir() throws IOException, CommandFailed
    {
        // given
        File configDirConfigFile = configDir.resolve( "neo4j.conf" ).toFile();
        appendToFile( configDirConfigFile, "causal_clustering.expected_core_cluster_size=4" );

        // and
        File homeDirConfigFile = homeDir.resolve( "neo4j.conf" ).toFile();
        appendToFile( homeDirConfigFile, "causal_clustering.expected_core_cluster_size=5\n" );
        appendToFile( homeDirConfigFile, "causal_clustering.raft_in_queue_max_batch=21" );

        // when
        Config config  = subject.loadConfig( Optional.empty() );

        // then
        assertEquals( Integer.valueOf( 4 ), config.get( CausalClusteringSettings.expected_core_cluster_size ) );
        assertEquals( Integer.valueOf( 64 ), config.get( CausalClusteringSettings.raft_in_queue_max_batch ) );
    }

    @Test
    public void prioritiseAdditionalOverConfigDir() throws IOException, CommandFailed
    {
        // given
        File configDirConfigFile = configDir.resolve( "neo4j.conf" ).toFile();
        appendToFile( configDirConfigFile, "causal_clustering.expected_core_cluster_size=4\n" );
        appendToFile( configDirConfigFile, "causal_clustering.raft_in_queue_max_batch=21" );

        // and
        File additionalConfigFile = testDirectory.file( "additional-neo4j.conf" );
        appendToFile( additionalConfigFile, "causal_clustering.expected_core_cluster_size=5" );

        // when
        Config config = subject.loadConfig( Optional.of( additionalConfigFile.toPath() ) );

        // then
        assertEquals( Integer.valueOf( 5 ), config.get( CausalClusteringSettings.expected_core_cluster_size ) );
        assertEquals( Integer.valueOf( 21 ), config.get( CausalClusteringSettings.raft_in_queue_max_batch ) );
    }

    private static void appendToFile( File file, String string ) throws IOException
    {
        BufferedWriter bufferedWriter = new BufferedWriter( new FileWriter( file, true ) );
        bufferedWriter.append( string );
        bufferedWriter.close();
    }
}
