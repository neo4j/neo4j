/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.security.enterprise.auth;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.kernel.enterprise.api.security.EnterpriseSecurityContext;
import org.neo4j.server.security.enterprise.auth.AuthProceduresBase.UserResult;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SecurityProceduresTest
{

    private SecurityProcedures procedures;

    @Before
    public void setup()
    {
        AuthSubject subject = mock( AuthSubject.class );
        when( subject.username() ).thenReturn( "pearl" );

        EnterpriseSecurityContext ctx = mock( EnterpriseSecurityContext.class );
        when( ctx.subject() ).thenReturn( subject );
        when( ctx.roles() ).thenReturn( Collections.singleton( "jammer" ) );

        procedures = new SecurityProcedures();
        procedures.securityContext = ctx;
        procedures.userManager = mock( EnterpriseUserManager.class );
    }

    @Test
    public void shouldReturnSecurityContextRoles()
    {
        List<UserResult> infoList = procedures.showCurrentUser().collect( Collectors.toList() );
        assertThat( infoList.size(), equalTo(1) );

        UserResult row = infoList.get( 0 );
        assertThat( row.username, equalTo( "pearl" ) );
        assertThat( row.roles, containsInAnyOrder( "jammer" ) );
        assertThat( row.flags, empty() );
    }
}
