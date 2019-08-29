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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;

import org.neo4j.cli.ExecutionContext;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.security.UserManager;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.security.auth.CommunitySecurityModule;
import org.neo4j.server.security.auth.FileUserRepository;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

@EphemeralTestDirectoryExtension
class SetInitialPasswordCommandTest
{
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private TestDirectory testDir;

    private SetInitialPasswordCommand command;
    private File authInitFile;

    @BeforeEach
    void setup()
    {
        command = new SetInitialPasswordCommand( new ExecutionContext( testDir.directory( "home" ).toPath(),
                testDir.directory( "conf" ).toPath(), mock( PrintStream.class ), mock( PrintStream.class ), fileSystem ) );

        authInitFile = CommunitySecurityModule.getInitialUserRepositoryFile( command.loadNeo4jConfig() );
        CommunitySecurityModule.getUserRepositoryFile( command.loadNeo4jConfig() );
    }

    @Test
    void printUsageHelp()
    {
        final var baos = new ByteArrayOutputStream();
        try ( var out = new PrintStream( baos ) )
        {
            CommandLine.usage( command, new PrintStream( out ) );
        }
        assertThat( baos.toString().trim(), equalTo( String.format(
                "USAGE%n" +
                "%n" +
                "set-initial-password [--verbose] <password>%n" +
                "%n" +
                "DESCRIPTION%n" +
                "%n" +
                "Sets the initial password of the initial admin user ('neo4j').%n" +
                "%n" +
                "PARAMETERS%n" +
                "%n" +
                "      <password>%n" +
                "%n" +
                "OPTIONS%n" +
                "%n" +
                "      --verbose    Enable verbose output."
        ) ) );
    }

    @Test
    void shouldSetInitialPassword() throws Throwable
    {
        // Given
        assertFalse( fileSystem.fileExists( authInitFile ) );

        // When
        CommandLine.populateCommand( command, "123" );
        command.execute();

        // Then
        assertAuthIniFile( "123" );
    }

    @Test
    void shouldOverwriteInitialPasswordFileIfExists() throws Throwable
    {
        // Given
        fileSystem.mkdirs( authInitFile.getParentFile() );
        fileSystem.write( authInitFile );

        // When
        CommandLine.populateCommand( command, "123" );
        command.execute();

        // Then
        assertAuthIniFile( "123" );
    }

    @Test
    void shouldWorkAlsoWithSamePassword() throws Throwable
    {
        CommandLine.populateCommand( command, "neo4j" );
        command.execute();

        // Then
        assertAuthIniFile( "neo4j" );
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
