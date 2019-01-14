/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.server.rest.security;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.server.rest.dbms.UserServiceTest;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.enterprise.auth.MultiRealmAuthManagerRule;
import org.neo4j.server.security.enterprise.auth.ShiroSubject;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EnterpriseUserServiceTest extends UserServiceTest
{
    @Rule
    public MultiRealmAuthManagerRule authManagerRule = new MultiRealmAuthManagerRule(
            userRepository,
            mock( AuthenticationStrategy.class )
        );

    @Override
    protected void setupAuthManagerAndSubject()
    {
        userManagerSupplier = authManagerRule.getManager();

        ShiroSubject shiroSubject = mock( ShiroSubject.class );
        when( shiroSubject.getPrincipal() ).thenReturn( "neo4j" );
        neo4jContext = authManagerRule.makeLoginContext( shiroSubject );
    }

    @Test
    public void shouldLogPasswordChange() throws Exception
    {
        shouldChangePasswordAndReturnSuccess();

        MultiRealmAuthManagerRule.FullSecurityLog fullLog = authManagerRule.getFullSecurityLog();
        fullLog.assertHasLine( "neo4j", "changed password" );
    }

    @Test
    public void shouldLogFailedPasswordChange() throws Exception
    {
        shouldReturn422IfPasswordIdentical();

        MultiRealmAuthManagerRule.FullSecurityLog fullLog = authManagerRule.getFullSecurityLog();
        fullLog.assertHasLine( "neo4j", "tried to change password: Old password and new password cannot be the same." );
    }
}
