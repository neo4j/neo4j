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

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Collections;

import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.security.enterprise.auth.FileRoleRepository;
import org.neo4j.server.security.enterprise.auth.RoleRecord;

import static org.junit.Assert.assertThat;

public class EnterpriseUsersCommandIT extends UsersCommandIT
{

    @Before
    public void setup()
    {
        super.setup();
        // the following line ensures that the test setup code (like creating test users) works on the same initial
        // environment that the actual tested commands will encounter. In particular some auth state is created
        // on demand in both the UserCommand and in the real server. We want that state created before the tests
        // are run.
        tool.execute( graphDir.toPath(), confDir.toPath(), makeArgs( "users", "list" ) );
        tool.execute( graphDir.toPath(), confDir.toPath(), makeArgs( "roles", "list" ) );
        resetOutsideWorldMock();
    }

    @Test
    public void shouldRemoveUserFromRoleWhenRemovingUser() throws Throwable
    {
        // given
        createTestRole( "test_role" );
        createTestUser( "another", "abc" );
        assertSuccessfulSubCommand( "roles", "assign", args( "test_role", "another" ),
                "Assigned role 'test_role' to user 'another'" );
        assertSuccessfulSubCommand( "roles", "users", args( "test_role" ), "another" );

        // when
        assertSuccessfulSubCommand( "users", "delete", args( "another" ), "Deleted user 'another'" );

        // then
        assertThat( getRole( "test_role" ).users(), org.hamcrest.core.IsEqual.equalTo( Collections.emptySortedSet() ) );
    }

    private File rolesFile()
    {
        return new File( new File( new File( graphDir, "data" ), "dbms" ), "roles" );
    }

    private RoleRecord createTestRole( String roleName ) throws Throwable
    {
        FileRoleRepository roles = new FileRoleRepository( fileSystem, rolesFile(), NullLogProvider.getInstance() );
        roles.start();
        RoleRecord role = new RoleRecord.Builder().withName( roleName ).build();
        roles.create( role );
        return role;
    }

    private RoleRecord getRole( String roleName ) throws Throwable
    {
        FileRoleRepository roles = new FileRoleRepository( fileSystem, rolesFile(), NullLogProvider.getInstance() );
        roles.start();
        return roles.getRoleByName( roleName );
    }
}
