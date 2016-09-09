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

import org.neo4j.server.security.auth.User;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

class UsersCommandTestBase extends CommandTestBase
{
    protected static String password_change_required = "password_change_required";

    @Override
    protected String command()
    {
        return "users";
    }

    void assertUserRequiresPasswordChange( String username ) throws Throwable
    {
        User user = getUser( username );
        assertThat( "User should require password change", user.getFlags(), hasItem( password_change_required ) );
    }

    void assertUserDoesNotRequirePasswordChange( String username ) throws Throwable
    {
        User user = getUser( username );
        assertThat( "User should not require password change", user.getFlags(),
                not( hasItem( password_change_required ) ) );
    }

}
