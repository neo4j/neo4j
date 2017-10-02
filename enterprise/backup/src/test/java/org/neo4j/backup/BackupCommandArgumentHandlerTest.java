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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class BackupCommandArgumentHandlerTest
{
    @Rule
    public ExpectedException expected = ExpectedException.none();

    BackupCommandArgumentHandler subject;

    @Before
    public void setup()
    {
        subject = new BackupCommandArgumentHandler( );
    }

    @Test
    public void unspecifiedHostnameIsEmptyOptional() throws CommandFailed, IncorrectUsage, BackupTool.ToolFailureException
    {
        OnlineBackupRequiredArguments requiredArguments = subject.establishRequiredArguments( requiredArgumentsWithAdditional( "--from=:1234" ) );

        assertFalse( requiredArguments.getAddress().getHostname().isPresent() );
        assertEquals( 1234, requiredArguments.getAddress().getPort().get().intValue() );
    }

    @Test
    public void unspecifiedPortIsEmptyOptional() throws IncorrectUsage
    {
        OnlineBackupRequiredArguments requiredArguments = subject.establishRequiredArguments( requiredArgumentsWithAdditional( "--from=abc" ) );

        assertEquals( "abc", requiredArguments.getAddress().getHostname().get() );
        assertFalse( requiredArguments.getAddress().getPort().isPresent() );
    }

    @Test
    public void acceptHostWithTrailingPort() throws CommandFailed, IncorrectUsage, BackupTool.ToolFailureException
    {
        OnlineBackupRequiredArguments requiredArguments = subject.establishRequiredArguments( requiredArgumentsWithAdditional( "--from=foo.bar.server:" ) );
        assertEquals( "foo.bar.server", requiredArguments.getAddress().getHostname().get() );
        assertFalse( requiredArguments.getAddress().getPort().isPresent() );
    }

    @Test
    public void acceptPortWithPrecedingEmptyHost() throws CommandFailed, IncorrectUsage, BackupTool.ToolFailureException
    {
        OnlineBackupRequiredArguments requiredArguments = subject.establishRequiredArguments( requiredArgumentsWithAdditional( "--from=:1234" ) );
        assertFalse( requiredArguments.getAddress().getHostname().isPresent() );
        assertEquals( 1234, requiredArguments.getAddress().getPort().get().intValue() );
    }

    @Test
    public void acceptBothIfSpecified() throws CommandFailed, IncorrectUsage, BackupTool.ToolFailureException
    {
        OnlineBackupRequiredArguments requiredArguments = subject.establishRequiredArguments( requiredArgumentsWithAdditional( "--from=foo.bar.server:1234" ) );
        assertEquals( "foo.bar.server", requiredArguments.getAddress().getHostname().get() );
        assertEquals( 1234, requiredArguments.getAddress().getPort().get().intValue() );
    }

    @Test
    public void backupDirectoryArgumentIsMandatory() throws Exception
    {
        expected.expect( IncorrectUsage.class );
        expected.expectMessage( "Missing argument 'backup-dir'" );
        subject.establishRequiredArguments();
    }

    @Test
    public void shouldDefaultTimeoutToTwentyMinutes() throws BackupTool.ToolFailureException, CommandFailed, IncorrectUsage
    {
        OnlineBackupRequiredArguments requiredArguments = subject.establishRequiredArguments( "--backup-dir=/", "--name=mybackup" );

        assertEquals( MINUTES.toMillis( 20 ), requiredArguments.getTimeout() );
    }

    @Test
    public void shouldInterpretAUnitlessTimeoutAsSeconds() throws BackupTool.ToolFailureException, CommandFailed, IncorrectUsage
    {
        OnlineBackupRequiredArguments requiredArguments = subject.establishRequiredArguments( "--timeout=10", "--backup-dir=/", "--name=mybackup" );

        assertEquals( SECONDS.toMillis( 10 ), requiredArguments.getTimeout() );
    }

    @Test
    public void shouldParseATimeoutWithUnits() throws BackupTool.ToolFailureException, CommandFailed, IncorrectUsage
    {
        OnlineBackupRequiredArguments requiredArguments = subject.establishRequiredArguments( "--timeout=10h", "--backup-dir=/", "--name=mybackup" );

        assertEquals( HOURS.toMillis( 10 ), requiredArguments.getTimeout() );
    }

    @Test
    public void shouldTreatNameArgumentAsMandatory() throws Exception
    {
        expected.expect( IncorrectUsage.class );
        expected.expectMessage( "Missing argument 'name'" );

        subject.establishRequiredArguments( "--backup-dir=/" );
    }

    @Test
    public void reportDirMustBeAPath() throws Exception
    {
        expected.expect( IncorrectUsage.class );
        expected.expectMessage( "cc-report-dir must be a path" );
        subject.establishRequiredArguments( "--check-consistency", "--backup-dir=/", "--name=mybackup", "--cc-report-dir" );
    }

    private String[] requiredArgumentsWithAdditional( String... additionalArgs )
    {
        List<String> args = new ArrayList<>();
        args.addAll( Arrays.asList( "--backup-dir=/", "--name=mybackup" ) );
        args.addAll( Arrays.asList( additionalArgs ) );
        String[] actualArgs = new String[args.size()];
        args.toArray( actualArgs );
        return actualArgs;
    }
}
