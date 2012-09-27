/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.withSettings;

import java.io.PrintStream;

import org.junit.Test;

public class BackupToolTest
{
    @Test
    public void runsBackup() throws Exception
    {
        // given
        String[] args = new String[]{"-full", "-from", "single://localhost", "-to", "my_backup"};
        BackupService service = mock( BackupService.class );
        PrintStream systemOut = mock( PrintStream.class );

        // when
        new BackupTool( service, systemOut, null ).run( args );

        // then
        verify( service ).doFullBackup( eq( "localhost" ), eq( BackupServer.DEFAULT_PORT ),
                eq( "my_backup" ), eq( true ) );
        verify( systemOut ).println( "Performing full backup from 'single://localhost'" );
        verify( systemOut ).println( "Done" );
    }

    @Test
    public void exitWithFailureIfNoModeSpecified() throws Exception
    {
        // given
        String[] args = new String[]{"-from", "single://localhost", "-to", "my_backup"};
        BackupService service = mock( BackupService.class );
        PrintStream systemOut = mock( PrintStream.class );
        BackupTool.Runtime runtime = mock( SystemExitAsException.class,
                withSettings().defaultAnswer( CALLS_REAL_METHODS ) );
        BackupTool backupTool = new BackupTool( service, systemOut, runtime );

        try
        {
            // when
            backupTool.run( args );
            fail( "should exit abnormally");
        }
        catch ( SystemExit e )
        {
            // expected
        }

        // then
        verify( runtime ).exitAbnormally( "Specify either -full or -incremental" );
        verifyZeroInteractions( service );
    }

    @Test
    public void exitWithFailureIfBothModesSpecified() throws Exception
    {
        // given
        String[] args = new String[]{"-full", "-incremental", "-from", "single://localhost", "-to", "my_backup"};
        BackupService service = mock( BackupService.class );
        PrintStream systemOut = mock( PrintStream.class );
        BackupTool.Runtime runtime = mock( SystemExitAsException.class,
                withSettings().defaultAnswer( CALLS_REAL_METHODS ) );
        BackupTool backupTool = new BackupTool( service, systemOut, runtime );

        try
        {
            // when
            backupTool.run( args );
            fail( "should exit abnormally");
        }
        catch ( SystemExit e )
        {
            // expected
        }

        // then
        verify( runtime ).exitAbnormally( "Specify either -full or -incremental" );
        verifyZeroInteractions( service );
    }

    @Test
    public void exitWithFailureIfNoSourceSpecified() throws Exception
    {
        // given
        String[] args = new String[]{"-full", "-to", "my_backup"};
        BackupService service = mock( BackupService.class );
        PrintStream systemOut = mock( PrintStream.class );
        BackupTool.Runtime runtime = mock( SystemExitAsException.class,
                withSettings().defaultAnswer( CALLS_REAL_METHODS ) );
        BackupTool backupTool = new BackupTool( service, systemOut, runtime );

        try
        {
            // when
            backupTool.run( args );
            fail( "should exit abnormally");
        }
        catch ( SystemExit e )
        {
            // expected
        }

        // then
        verify( runtime ).exitAbnormally( "Please specify -from, examples:\n" +
                "  -from single://192.168.1.34\n" +
                "  -from single://192.168.1.34:1234\n" +
                "  -from ha://192.168.1.15:2181\n" +
                "  -from ha://192.168.1.15:2181,192.168.1.16:2181" );
        verifyZeroInteractions( service );
    }

    @Test
    public void exitWithFailureIfInvalidSourceSpecified() throws Exception
    {
        // given
        String[] args = new String[]{"-full", "-from", "foo:localhost:123", "-to", "my_backup"};
        BackupService service = mock( BackupService.class );
        PrintStream systemOut = mock( PrintStream.class );
        BackupTool.Runtime runtime = mock( SystemExitAsException.class,
                withSettings().defaultAnswer( CALLS_REAL_METHODS ) );
        BackupTool backupTool = new BackupTool( service, systemOut, runtime );

        try
        {
            // when
            backupTool.run( args );
            fail( "should exit abnormally");
        }
        catch ( SystemExit e )
        {
            // expected
        }

        // then
        verify( runtime ).exitAbnormally( "foo was specified as a backup module but it was not found. " +
                "Please make sure that the implementing service is on the classpath." );
        verifyZeroInteractions( service );
    }

    @Test
    public void exitWithFailureIfNoDestinationSpecified() throws Exception
    {
        // given
        String[] args = new String[]{"-full", "-from", "single://localhost"};
        BackupService service = mock( BackupService.class );
        PrintStream systemOut = mock( PrintStream.class );
        BackupTool.Runtime runtime = mock( SystemExitAsException.class,
                withSettings().defaultAnswer( CALLS_REAL_METHODS ) );
        BackupTool backupTool = new BackupTool( service, systemOut, runtime );

        try
        {
            // when
            backupTool.run( args );
            fail( "should exit abnormally");
        }
        catch ( SystemExit e )
        {
            // expected
        }

        // then
        verify( runtime ).exitAbnormally( "Specify target location with -to <target-directory>" );
        verifyZeroInteractions( service );
    }

    public static class SystemExitAsException extends BackupTool.Runtime
    {
        @Override
        void exitAbnormally( String message, Exception ex )
        {
            throw new SystemExit();
        }

        @Override
        void exitAbnormally( String message )
        {
            throw new SystemExit();
        }
    }

    static class SystemExit extends RuntimeException
    {
    }

}
