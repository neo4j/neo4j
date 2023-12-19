/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
