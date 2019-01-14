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
package org.neo4j.commandline.admin.security;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;

import org.neo4j.commandline.admin.CommandLocator;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.admin.Usage;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.security.UserManager;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.security.auth.CommunitySecurityModule;
import org.neo4j.server.security.auth.FileUserRepository;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.test.assertion.Assert.assertException;

public class SetInitialPasswordCommandTest
{
    private SetInitialPasswordCommand setPasswordCommand;
    private File authInitFile;
    private File authFile;
    private FileSystemAbstraction fileSystem;

    private final EphemeralFileSystemRule fileSystemRule = new EphemeralFileSystemRule();
    private final TestDirectory testDir = TestDirectory.testDirectory( fileSystemRule.get() );

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( fileSystemRule ).around( testDir );

    @Before
    public void setup()
    {
        fileSystem = fileSystemRule.get();
        OutsideWorld mock = mock( OutsideWorld.class );
        when( mock.fileSystem() ).thenReturn( fileSystem );
        setPasswordCommand = new SetInitialPasswordCommand( testDir.directory( "home" ).toPath(),
                testDir.directory( "conf" ).toPath(), mock );
        authInitFile = CommunitySecurityModule.getInitialUserRepositoryFile( setPasswordCommand.loadNeo4jConfig() );
        CommunitySecurityModule.getUserRepositoryFile( setPasswordCommand.loadNeo4jConfig() );
    }

    @Test
    public void shouldFailSetPasswordWithNoArguments()
    {
        assertException( () -> setPasswordCommand.execute( new String[0] ), IncorrectUsage.class,
                "not enough arguments" );
    }

    @Test
    public void shouldFailSetPasswordWithTooManyArguments()
    {
        String[] arguments = {"", "123", "321"};
        assertException( () -> setPasswordCommand.execute( arguments ), IncorrectUsage.class, "unrecognized arguments: '123 321'" );
    }

    @Test
    public void shouldSetInitialPassword() throws Throwable
    {
        // Given
        assertFalse( fileSystem.fileExists( authInitFile ) );

        // When
        String[] arguments = {"123"};
        setPasswordCommand.execute( arguments );

        // Then
        assertAuthIniFile( "123" );
    }

    @Test
    public void shouldOverwriteInitialPasswordFileIfExists() throws Throwable
    {
        // Given
        fileSystem.mkdirs( authInitFile.getParentFile() );
        fileSystem.create( authInitFile );

        // When
        String[] arguments = {"123"};
        setPasswordCommand.execute( arguments );

        // Then
        assertAuthIniFile( "123" );
    }

    @Test
    public void shouldWorkAlsoWithSamePassword() throws Throwable
    {
        String[] arguments = {"neo4j"};
        setPasswordCommand.execute( arguments );

        // Then
        assertAuthIniFile( "neo4j" );
    }

    @Test
    public void shouldPrintNiceHelp() throws Throwable
    {
        try ( ByteArrayOutputStream baos = new ByteArrayOutputStream() )
        {
            PrintStream ps = new PrintStream( baos );

            Usage usage = new Usage( "neo4j-admin", mock( CommandLocator.class ) );
            usage.printUsageForCommand( new SetInitialPasswordCommandProvider(), ps::println );

            assertEquals( String.format( "usage: neo4j-admin set-initial-password <password>%n" +
                            "%n" +
                            "environment variables:%n" +
                            "    NEO4J_CONF    Path to directory which contains neo4j.conf.%n" +
                            "    NEO4J_DEBUG   Set to anything to enable debug output.%n" +
                            "    NEO4J_HOME    Neo4j home directory.%n" +
                            "    HEAP_SIZE     Set JVM maximum heap size during command execution.%n" +
                            "                  Takes a number and a unit, for example 512m.%n" +
                            "%n" +
                            "Sets the initial password of the initial admin user ('neo4j').%n" ),
                    baos.toString() );
        }
    }

    private void assertAuthIniFile( String password ) throws Throwable
    {
        assertTrue( fileSystem.fileExists( authInitFile ) );
        FileUserRepository userRepository = new FileUserRepository( fileSystem, authInitFile,
                NullLogProvider.getInstance() );
        userRepository.start();
        User neo4j = userRepository.getUserByName( UserManager.INITIAL_USER_NAME );
        assertNotNull( neo4j );
        assertTrue( neo4j.credentials().matchesPassword( password ) );
        assertFalse( neo4j.hasFlag( User.PASSWORD_CHANGE_REQUIRED ) );
    }
}
