/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.commandline.admin.security;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;

import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.test.rule.TargetDirectory;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class ListTransactionsCommandTest
{
    private TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( testDir );

    @Test
    public void shouldFailWithNoArguments() throws Exception
    {
        ListTransactionsCommand setPasswordCommand = new ListTransactionsCommand( testDir.directory( "home" ).toPath(),
                testDir.directory( "conf" ).toPath() );

        String[] arguments = {};
        try
        {
            setPasswordCommand.execute( arguments );
            fail( "Should have thrown an exception." );
        }
        catch ( IncorrectUsage e )
        {
            assertThat( e.getMessage(), containsString( "Missing argument" ) );
        }
    }

    @Test
    public void shouldFailWithoutPassword() throws Exception
    {
        ListTransactionsCommand setPasswordCommand = new ListTransactionsCommand( testDir.directory( "home" ).toPath(),
                testDir.directory( "conf" ).toPath() );

        String[] arguments = {"--username=neo4j"};
        try
        {
            setPasswordCommand.execute( arguments );
            fail( "Should have thrown an exception." );
        }
        catch ( IncorrectUsage e )
        {
            assertThat( e.getMessage(), containsString( "password" ) );
        }
    }

    @Test
    public void shouldFailWithoutUsername() throws Exception
    {
        ListTransactionsCommand setPasswordCommand = new ListTransactionsCommand( testDir.directory( "home" ).toPath(),
                testDir.directory( "conf" ).toPath() );

        String[] arguments = {"--password=neo4j"};
        try
        {
            setPasswordCommand.execute( arguments );
            fail( "Should have thrown an exception." );
        }
        catch ( IncorrectUsage e )
        {
            assertThat( e.getMessage(), containsString( "username" ) );
        }
    }

    @Test
    public void shouldRunListTransactionsCommand() throws Throwable
    {
        // Given
        File graphDir = testDir.graphDbDir();
        File confDir = new File( graphDir, "conf" );
        //TODO: Start database

        // When - the admin command sets the password
        ListTransactionsCommand listTransactionsCommand = new ListTransactionsCommand( graphDir.toPath(), confDir.toPath() );
        listTransactionsCommand.execute( new String[]{"--username=neo4j", "--password=abc"} );

        // Then - it works
        //TODO:
    }

}
