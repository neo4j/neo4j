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
package org.neo4j.server.security.enterprise.auth.plugin.api;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class AuthTokenTest
{
    @Test
    public void shouldMatchKernelSecurityApiConstants()
    {
        // If you change one of these you need to change the other!
        assertThat( AuthToken.PRINCIPAL, equalTo( org.neo4j.kernel.api.security.AuthToken.PRINCIPAL ) );
        assertThat( AuthToken.CREDENTIALS, equalTo( org.neo4j.kernel.api.security.AuthToken.CREDENTIALS ) );
        assertThat( AuthToken.REALM, equalTo( org.neo4j.kernel.api.security.AuthToken.REALM_KEY ) );
        assertThat( AuthToken.PARAMETERS, equalTo( org.neo4j.kernel.api.security.AuthToken.PARAMETERS ) );
    }
}
