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

import java.io.File;
import java.io.IOException;

import org.neo4j.kernel.api.security.exception.InvalidArgumentsException;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.security.auth.Credential;
import org.neo4j.server.security.auth.FileUserRepository;
import org.neo4j.server.security.auth.User;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

public class CommandTestBase
{
    protected static String password_change_required = "password_change_required";
    protected TestDirectory testDir = TestDirectory.testDirectory();

    protected File authFile()
    {
        return new File( new File( new File( testDir.graphDbDir(), "data" ), "dbms" ), "auth" );
    }

    protected User createTestUser( String username, String password ) throws IOException, InvalidArgumentsException
    {
        FileUserRepository users = new FileUserRepository( authFile().toPath(), NullLogProvider.getInstance() );
        User user =
                new User.Builder( username, Credential.forPassword( password ) ).withRequiredPasswordChange( true )
                        .build();
        users.create( user );
        return user;
    }

    protected User getUser( String username ) throws Throwable
    {
        FileUserRepository afterUsers =
                new FileUserRepository( authFile().toPath(), NullLogProvider.getInstance() );
        afterUsers.start(); // load users from disk
        return afterUsers.getUserByName( username );
    }

    protected void assertUserRequiresPasswordChange( String username ) throws Throwable
    {
        User user = getUser( username );
        assertThat( "User should require password change", user.getFlags(), hasItem( password_change_required ) );

    }

    protected void assertUserDoesNotRequirePasswordChange( String username ) throws Throwable
    {
        User user = getUser( username );
        assertThat( "User should not require password change", user.getFlags(),
                not( hasItem( password_change_required ) ) );

    }
}
