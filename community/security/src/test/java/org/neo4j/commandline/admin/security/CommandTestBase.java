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
package org.neo4j.commandline.admin.security;

import org.junit.Before;

import java.io.File;
import java.nio.file.FileSystems;

import org.neo4j.commandline.admin.AdminTool;
import org.neo4j.commandline.admin.CommandLocator;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.io.fs.DelegateFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.security.auth.Credential;
import org.neo4j.server.security.auth.FileUserRepository;
import org.neo4j.server.security.auth.User;
import org.neo4j.test.rule.TestDirectory;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

abstract class CommandTestBase
{
    protected TestDirectory testDir = TestDirectory.testDirectory();
    protected FileSystemAbstraction fileSystem = new DelegateFileSystemAbstraction( FileSystems.getDefault() );

    File graphDir;
    File confDir;
    File homeDir;
    OutsideWorld out;
    AdminTool tool;

    @Before
    public void setup()
    {
        graphDir = testDir.graphDbDir();
        confDir = ensureDir( "conf" );
        homeDir = ensureDir( "home" );
        out = mock( OutsideWorld.class );
        resetOutsideWorldMock();
        tool = new AdminTool( CommandLocator.fromServiceLocator(), out, true );
    }

    protected File ensureDir( String name )
    {
        File dir = new File( graphDir, name );
        if ( !dir.exists() )
        {
            dir.mkdirs();
        }
        return dir;
    }

    protected void resetOutsideWorldMock()
    {
        reset(out);
        when( out.fileSystem() ).thenReturn( fileSystem );
    }

    private File authFile()
    {
        return new File( new File( new File( testDir.graphDbDir(), "data" ), "dbms" ), "auth" );
    }

    User createTestUser( String username, String password ) throws Throwable
    {
        FileUserRepository users = new FileUserRepository( fileSystem, authFile(), NullLogProvider.getInstance() );
        users.start();
        User user =
                new User.Builder( username, Credential.forPassword( password ) ).withRequiredPasswordChange( true )
                        .build();
        users.create( user );
        return user;
    }

    User getUser( String username ) throws Throwable
    {
        FileUserRepository afterUsers =
                new FileUserRepository( fileSystem, authFile(), NullLogProvider.getInstance() );
        afterUsers.start(); // load users from disk
        return afterUsers.getUserByName( username );
    }

    String[] args(String... args)
    {
        return args;
    }

    protected String[] makeArgs( String command, String subCommand, String... args )
    {
        String[] allArgs;
        if ( subCommand.isEmpty() ) {
            allArgs = new String[1];
        }
        else
        {
            allArgs = new String[args.length + 2];
            System.arraycopy( args, 0, allArgs, 2, args.length );
            allArgs[1] = subCommand;
        }
        allArgs[0] = command;
        return allArgs;
    }

    void assertFailedSubCommand( String command, String subCommand, String[] args, String... errors )
    {
        resetOutsideWorldMock();
        tool.execute( graphDir.toPath(), confDir.toPath(), makeArgs( command, subCommand, args ) );

        // Then we get the expected error
        for ( String error : errors )
        {
            verify( out ).stdErrLine( contains( error ) );
        }
        verify( out, times( 0 ) ).stdOutLine( anyString() );
        verify( out ).exit( 1 );
    }

    void assertSuccessfulSubCommand( String command, String subCommand, String[] args, String... messages )
    {
        resetOutsideWorldMock();
        tool.execute( graphDir.toPath(), confDir.toPath(), makeArgs( command, subCommand, args ));

        // Then we get the expected output messages
        for ( String message : messages )
        {
            verify( out ).stdOutLine( contains( message ) );
        }
        verify( out, times( 0 ) ).stdErrLine( anyString() );
        verify( out ).exit( 0 );
    }

}
