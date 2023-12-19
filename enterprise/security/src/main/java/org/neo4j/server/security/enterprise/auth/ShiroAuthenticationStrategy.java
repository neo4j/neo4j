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

import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.pam.AbstractAuthenticationStrategy;
import org.apache.shiro.realm.Realm;

import java.util.Collection;

public class ShiroAuthenticationStrategy extends AbstractAuthenticationStrategy
{
    @Override
    public AuthenticationInfo beforeAllAttempts( Collection<? extends Realm> realms, AuthenticationToken token )
            throws AuthenticationException
    {
        return new ShiroAuthenticationInfo();
    }

    @Override
    public AuthenticationInfo afterAttempt( Realm realm, AuthenticationToken token, AuthenticationInfo singleRealmInfo,
            AuthenticationInfo aggregateInfo, Throwable t ) throws AuthenticationException
    {
        AuthenticationInfo info = super.afterAttempt( realm, token, singleRealmInfo, aggregateInfo, t );
        if ( t != null && info instanceof ShiroAuthenticationInfo )
        {
            // Save the throwable so we can use it for correct log messages later
            ((ShiroAuthenticationInfo) info).addThrowable( t );
        }
        return info;
    }
}
