/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import java.nio.file.Path;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CheckConsistencyCommandTest
{
    private TestDirectory testDir = TestDirectory.testDirectory( getClass() );

    @Rule
    public final DatabaseRule db = new EmbeddedDatabaseRule();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( testDir );

    @Test
    public void requiresDatabaseArgument() throws Exception
    {
        OutsideWorld outsideWorld = mock( OutsideWorld.class );
        FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( testDir.directory( "home" ).toPath(), testDir.directory( "conf" ).toPath(),
                        outsideWorld, fs );

        String[] arguments = {""};
        try
        {
            checkConsistencyCommand.execute( arguments );
            fail( "Should have thrown an exception." );
        }
        catch ( IncorrectUsage e )
        {
            assertThat( e.getMessage(), containsString( "database" ) );
        }
    }

    @Test
    public void runsConsistencyChecker() throws Exception
    {
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        Path homeDir = testDir.directory( "home" ).toPath();
        OutsideWorld outsideWorld = mock( OutsideWorld.class );
        FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( homeDir, testDir.directory( "conf" ).toPath(), outsideWorld, fs,
                        consistencyCheckService );

        File databasePath = new File( homeDir.toFile(), "data/databases/mydb" );

        when( consistencyCheckService
                .runFullConsistencyCheck( eq( databasePath ), any( Config.class ), any( ProgressMonitorFactory.class ),
                        any( LogProvider.class ), any( FileSystemAbstraction.class ), eq( false ) ) )
                .thenReturn( ConsistencyCheckService.Result.SUCCESS );

        checkConsistencyCommand.execute( new String[]{"--database=mydb"} );

        verify( consistencyCheckService )
                .runFullConsistencyCheck( eq( databasePath ), any( Config.class ), any( ProgressMonitorFactory.class ),
                        any( LogProvider.class ), any( FileSystemAbstraction.class ), eq( false ) );
    }

    @Test
    public void enablesVerbosity() throws Exception
    {
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        Path homeDir = testDir.directory( "home" ).toPath();
        OutsideWorld outsideWorld = mock( OutsideWorld.class );
        FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( homeDir, testDir.directory( "conf" ).toPath(), outsideWorld, fs,
                        consistencyCheckService );

        File databasePath = new File( homeDir.toFile(), "data/databases/mydb" );

        when( consistencyCheckService
                .runFullConsistencyCheck( eq( databasePath ), any( Config.class ), any( ProgressMonitorFactory.class ),
                        any( LogProvider.class ), any( FileSystemAbstraction.class ), eq( true ) ) )
                .thenReturn( ConsistencyCheckService.Result.SUCCESS );

        checkConsistencyCommand.execute( new String[]{"--database=mydb", "--verbose"} );

        verify( consistencyCheckService )
                .runFullConsistencyCheck( eq( databasePath ), any( Config.class ), any( ProgressMonitorFactory.class ),
                        any( LogProvider.class ), any( FileSystemAbstraction.class ), eq( true ) );
    }

    @Test
    public void failsWhenInconsistenciesAreFound() throws Exception
    {
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        Path homeDir = testDir.directory( "home" ).toPath();
        OutsideWorld outsideWorld = mock( OutsideWorld.class );
        FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( homeDir, testDir.directory( "conf" ).toPath(), outsideWorld, fs,
                        consistencyCheckService );
        File databasePath = new File( homeDir.toFile(), "data/databases/mydb" );

        when( consistencyCheckService
                .runFullConsistencyCheck( eq( databasePath ), any( Config.class ), any( ProgressMonitorFactory.class ),
                        any( LogProvider.class ), any( FileSystemAbstraction.class ), eq( true ) ) )
                .thenReturn( ConsistencyCheckService.Result.FAILURE );
        when( consistencyCheckService.chooseReportPath( any(), any() ) )
                .thenReturn( testDir.directory( "report file" ) );

        try
        {
            checkConsistencyCommand.execute( new String[]{"--database=mydb", "--verbose"} );
        }
        catch ( Exception e )
        {
            assertEquals( CommandFailed.class, e.getClass() );
        }
    }
}
