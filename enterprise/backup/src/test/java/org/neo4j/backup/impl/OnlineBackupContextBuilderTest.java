/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.backup.impl;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.internal.matchers.ThrowableCauseMatcher.hasCause;
import static org.neo4j.backup.impl.SelectedBackupProtocol.ANY;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.logical_logs_location;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.pagecache_memory;

public class OnlineBackupContextBuilderTest
{
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();
    @Rule
    public ExpectedException expected = ExpectedException.none();
    @Rule
    public SuppressOutput suppress = SuppressOutput.suppressAll();

    private Path homeDir;
    private Path configDir;
    private Path configFile;

    @Before
    public void setUp() throws IOException
    {
        homeDir = testDirectory.directory( "home" ).toPath();
        configDir = testDirectory.directory( "config" ).toPath();
        configFile = configDir.resolve( "neo4j.conf" );
        String neo4jConfContents = "dbms.backup.address = localhost:1234";
        Files.write( configFile, singletonList( neo4jConfContents ) );
    }

    @Test
    public void unspecifiedHostnameIsEmptyOptional() throws Exception
    {
        OnlineBackupContextBuilder handler = new OnlineBackupContextBuilder( homeDir, configDir );
        OnlineBackupContext context = handler.createContext( requiredAnd( "--from=:1234" ) );
        OnlineBackupRequiredArguments requiredArguments = context.getRequiredArguments();

        assertFalse( requiredArguments.getAddress().getHostname().isPresent() );
        assertEquals( 1234, requiredArguments.getAddress().getPort().get().intValue() );
    }

    @Test
    public void unspecifiedPortIsEmptyOptional() throws Exception
    {
        OnlineBackupContextBuilder handler = new OnlineBackupContextBuilder( homeDir, configDir );
        OnlineBackupContext context = handler.createContext( requiredAnd( "--from=abc" ) );
        OnlineBackupRequiredArguments requiredArguments = context.getRequiredArguments();

        assertEquals( "abc", requiredArguments.getAddress().getHostname().get() );
        assertFalse( requiredArguments.getAddress().getPort().isPresent() );
    }

    @Test
    public void acceptHostWithTrailingPort() throws Exception
    {
        OnlineBackupContextBuilder handler = new OnlineBackupContextBuilder( homeDir, configDir );
        OnlineBackupContext context = handler.createContext( requiredAnd( "--from=foo.bar.server:" ) );
        OnlineBackupRequiredArguments requiredArguments = context.getRequiredArguments();
        assertEquals( "foo.bar.server", requiredArguments.getAddress().getHostname().get() );
        assertFalse( requiredArguments.getAddress().getPort().isPresent() );
    }

    @Test
    public void acceptPortWithPrecedingEmptyHost() throws Exception
    {
        OnlineBackupContextBuilder handler = new OnlineBackupContextBuilder( homeDir, configDir );
        OnlineBackupContext context = handler.createContext( requiredAnd( "--from=:1234" ) );
        OnlineBackupRequiredArguments requiredArguments = context.getRequiredArguments();
        assertFalse( requiredArguments.getAddress().getHostname().isPresent() );
        assertEquals( 1234, requiredArguments.getAddress().getPort().get().intValue() );
    }

    @Test
    public void acceptBothIfSpecified() throws Exception
    {
        OnlineBackupContextBuilder handler = new OnlineBackupContextBuilder( homeDir, configDir );
        OnlineBackupContext context = handler.createContext( requiredAnd( "--from=foo.bar.server:1234" ) );
        OnlineBackupRequiredArguments requiredArguments = context.getRequiredArguments();
        assertEquals( "foo.bar.server", requiredArguments.getAddress().getHostname().get() );
        assertEquals( 1234, requiredArguments.getAddress().getPort().get().intValue() );
    }

    @Test
    public void backupDirectoryArgumentIsMandatory() throws Exception
    {
        expected.expect( IncorrectUsage.class );
        expected.expectMessage( "Missing argument 'backup-dir'" );
        new OnlineBackupContextBuilder( homeDir, configDir ).createContext();
    }

    @Test
    public void shouldDefaultTimeoutToTwentyMinutes() throws Exception
    {
        OnlineBackupContextBuilder handler = new OnlineBackupContextBuilder( homeDir, configDir );
        OnlineBackupContext context = handler.createContext( "--backup-dir=/", "--name=mybackup" );
        OnlineBackupRequiredArguments requiredArguments = context.getRequiredArguments();

        assertEquals( MINUTES.toMillis( 20 ), requiredArguments.getTimeout() );
    }

    @Test
    public void shouldInterpretAUnitlessTimeoutAsSeconds() throws Exception
    {
        OnlineBackupContextBuilder handler = new OnlineBackupContextBuilder( homeDir, configDir );
        OnlineBackupContext context = handler.createContext( "--timeout=10", "--backup-dir=/", "--name=mybackup" );
        OnlineBackupRequiredArguments requiredArguments = context.getRequiredArguments();

        assertEquals( SECONDS.toMillis( 10 ), requiredArguments.getTimeout() );
    }

    @Test
    public void shouldParseATimeoutWithUnits() throws Exception
    {
        OnlineBackupContextBuilder handler = new OnlineBackupContextBuilder( homeDir, configDir );
        OnlineBackupContext context = handler.createContext( requiredAnd( "--timeout=10h" ) );
        OnlineBackupRequiredArguments requiredArguments = context.getRequiredArguments();

        assertEquals( HOURS.toMillis( 10 ), requiredArguments.getTimeout() );
    }

    @Test
    public void shouldTreatNameArgumentAsMandatory() throws Exception
    {
        expected.expect( IncorrectUsage.class );
        expected.expectMessage( "Missing argument 'name'" );

        OnlineBackupContextBuilder handler = new OnlineBackupContextBuilder( homeDir, configDir );
        handler.createContext( "--backup-dir=/" );
    }

    @Test
    public void reportDirMustBeAPath() throws Exception
    {
        expected.expect( IncorrectUsage.class );
        expected.expectMessage( "cc-report-dir must be a path" );
        OnlineBackupContextBuilder handler = new OnlineBackupContextBuilder( homeDir, configDir );
        handler.createContext( requiredAnd( "--check-consistency", "--cc-report-dir" ) );
    }

    @Test
    public void errorHandledForNonExistingAdditionalConfigFile() throws Exception
    {
        // given
        Path additionalConf = homeDir.resolve( "neo4j.conf" );

        // and
        expected.expect( CommandFailed.class );
        expected.expectCause( hasCause( any( NoSuchFileException.class ) ) );

        // expect
        OnlineBackupContextBuilder handler = new OnlineBackupContextBuilder( homeDir, configDir );
        handler.createContext( requiredAnd( "--additional-config=" + additionalConf ) );
    }

    @Test
    public void prioritiseConfigDirOverHomeDir() throws Exception
    {
        // given
        Files.write( configFile, singletonList( "causal_clustering.minimum_core_cluster_size_at_startup=4" ), WRITE );

        // and
        Path homeDirConfigFile = homeDir.resolve( "neo4j.conf" );
        Files.write( homeDirConfigFile, asList( "causal_clustering.minimum_core_cluster_size_at_startup=5", "causal_clustering.raft_in_queue_max_batch=21" ) );

        // when
        OnlineBackupContextBuilder handler = new OnlineBackupContextBuilder( homeDir, configDir );
        Config config = handler.createContext( requiredAnd() ).getConfig();

        // then
        assertEquals( Integer.valueOf( 3 ), config.get( CausalClusteringSettings.minimum_core_cluster_size_at_formation ) );
        assertEquals( Integer.valueOf( 64 ), config.get( CausalClusteringSettings.raft_in_queue_max_batch ) );
    }

    @Test
    public void prioritiseAdditionalOverConfigDir() throws Exception
    {
        // given
        Files.write( configFile, asList( "causal_clustering.minimum_core_cluster_size_at_startup=4", "causal_clustering.raft_in_queue_max_batch=21" ) );

        // and
        Path additionalConf = homeDir.resolve( "additional-neo4j.conf" );
        Files.write( additionalConf, singletonList( "causal_clustering.minimum_core_cluster_size_at_startup=5" ) );

        // when
        OnlineBackupContextBuilder handler = new OnlineBackupContextBuilder( homeDir, configDir );
        OnlineBackupContext context = handler.createContext( requiredAnd( "--additional-config=" + additionalConf ) );
        Config config = context.getConfig();

        // then
        assertEquals( Integer.valueOf( 3 ), config.get( CausalClusteringSettings.minimum_core_cluster_size_at_formation ) );
        assertEquals( Integer.valueOf( 21 ), config.get( CausalClusteringSettings.raft_in_queue_max_batch ) );
    }

    @Test
    public void mustIgnorePageCacheConfigInConfigFile() throws Exception
    {
        // given
        Files.write( configFile, singletonList( pagecache_memory.name() + "=42m" ) );

        // when
        OnlineBackupContextBuilder contextBuilder = new OnlineBackupContextBuilder( homeDir, configDir );
        OnlineBackupContext context = contextBuilder.createContext( requiredAnd() );

        // then
        assertThat( context.getConfig().get( pagecache_memory ), is( "8m" ) );
    }

    @Test
    public void mustIgnorePageCacheConfigInAdditionalConfigFile() throws Exception
    {
        // given
        Path additionalConf = homeDir.resolve( "additional-neo4j.conf" );
        Files.write( additionalConf, singletonList( pagecache_memory.name() + "=42m" ) );

        // when
        OnlineBackupContextBuilder builder = new OnlineBackupContextBuilder( homeDir, configDir );
        OnlineBackupContext context = builder.createContext( requiredAnd( "--additional-config=" + additionalConf ) );

        // then
        assertThat( context.getConfig().get( pagecache_memory ), is( "8m" ) );
    }

    @Test
    public void mustRespectPageCacheConfigFromCommandLineArguments() throws Exception
    {
        // when
        OnlineBackupContextBuilder builder = new OnlineBackupContextBuilder( homeDir, configDir );
        OnlineBackupContext context = builder.createContext( requiredAnd( "--pagecache=42m" ) );

        // then
        assertThat( context.getConfig().get( pagecache_memory ), is( "42m" ) );
    }

    @Test
    public void logsMustBePlacedInTargetBackupDirectory() throws Exception
    {
        // when
        String name = "mybackup";
        Path backupDir = homeDir.resolve( "poke" );
        Path backupPath = backupDir.resolve( name );
        Files.createDirectories( backupDir );
        OnlineBackupContextBuilder builder = new OnlineBackupContextBuilder( homeDir, configDir );
        OnlineBackupContext context = builder.createContext( "--backup-dir=" + backupDir, "--name=" + name );
        assertThat( context.getConfig().get( logical_logs_location ).getAbsolutePath(), is( backupPath.toString() ) );
    }

    @Test
    public void defaultProtocolIsAny() throws CommandFailed, IncorrectUsage
    {
        // given
        OnlineBackupContextBuilder builder = new OnlineBackupContextBuilder( homeDir, configDir );

        // when context resolved without proto override value
        OnlineBackupContext context = builder.createContext( requiredAnd() );

        // then
        assertEquals( ANY, context.getRequiredArguments().getSelectedBackupProtocol() );
    }

    @Test
    public void overrideWithLegacy() throws CommandFailed, IncorrectUsage
    {
        // with
        List<String> input = Arrays.asList( "common", "catchup" );
        List<SelectedBackupProtocol> expected = Arrays.asList( SelectedBackupProtocol.COMMON, SelectedBackupProtocol.CATCHUP );

        for ( int useCase = 0; useCase < input.size(); useCase++ )
        {
            // given
            OnlineBackupContextBuilder builder = new OnlineBackupContextBuilder( homeDir, configDir );

            // when
            OnlineBackupContext context = builder.createContext( requiredAnd( "--protocol=" + input.get( useCase ) ) );

            // then
            assertEquals( expected.get( useCase ), context.getRequiredArguments().getSelectedBackupProtocol() );
        }
    }

    @Test
    public void prometheusShouldBeDisabledToAvoidPortConflicts() throws CommandFailed, IncorrectUsage
    {
        OnlineBackupContext context = new OnlineBackupContextBuilder( homeDir, configDir ).createContext( requiredAnd() );
        assertEquals( Settings.FALSE, context.getConfig().getRaw().get( "metrics.prometheus.enabled" ) );
    }

    @Test
    public void ipv6CanBeProcessed() throws CommandFailed, IncorrectUsage
    {
        // given
        OnlineBackupContextBuilder builder = new OnlineBackupContextBuilder( homeDir, configDir );

        // when
        OnlineBackupContext context = builder.createContext( requiredAnd( "--from=[fd00:ce10::2]:6362" ) );

        // then
        assertEquals( "fd00:ce10::2", context.getRequiredArguments().getAddress().getHostname().get() );
        assertEquals( Integer.valueOf( 6362 ), context.getRequiredArguments().getAddress().getPort().get() );
    }

    private String[] requiredAnd( String... additionalArgs )
    {
        List<String> args = new ArrayList<>();
        args.add( "--backup-dir=/" );
        args.add( "--name=mybackup" );
        Collections.addAll( args, additionalArgs );
        return args.toArray( new String[0] );
    }
}
