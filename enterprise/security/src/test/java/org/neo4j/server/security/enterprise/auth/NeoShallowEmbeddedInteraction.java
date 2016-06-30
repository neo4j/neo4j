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
package org.neo4j.server.security.enterprise.auth;

import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;


import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AuthenticationResult;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.InMemoryUserRepository;
import org.neo4j.server.security.auth.RateLimitedAuthenticationStrategy;
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory;

import static org.neo4j.server.security.auth.SecurityTestUtils.authToken;

import static java.time.Clock.systemUTC;

class NeoShallowEmbeddedInteraction implements NeoInteractionLevel<EnterpriseAuthSubject>
{
    private GraphDatabaseAPI db;
    private MultiRealmAuthManager manager;
    private EnterpriseUserManager userManager;

    NeoShallowEmbeddedInteraction() throws Throwable
    {
        db = (GraphDatabaseAPI) new TestEnterpriseGraphDatabaseFactory().newImpermanentDatabase();
        InternalFlatFileRealm internalRealm =
                new InternalFlatFileRealm( new InMemoryUserRepository(), new InMemoryRoleRepository(),
                        new BasicPasswordPolicy(), new RateLimitedAuthenticationStrategy( systemUTC(), 3 ) );
        manager = new MultiRealmAuthManager( internalRealm, Collections.singletonList( internalRealm ) );
        manager.init();
        manager.start();
        userManager = manager.getUserManager();
    }

    @Override
    public EnterpriseUserManager getManager()
    {
        return userManager;
    }

    @Override
    public String executeQuery( EnterpriseAuthSubject subject, String call, Map<String,Object> params,
            Consumer<ResourceIterator<Map<String, Object>>> resultConsumer )
    {
        try ( Transaction tx = db.beginTransaction( KernelTransaction.Type.explicit, subject ) )
        {
            Map<String,Object> p = (params == null) ? Collections.emptyMap() : params;
            resultConsumer.accept( db.execute( call, p ) );
            tx.success();
            return "";
        }
        catch ( Exception e )
        {
            return e.getMessage();
        }
    }

    @Override
    public EnterpriseAuthSubject login( String username, String password ) throws Throwable
    {
        return manager.login( authToken( username, password ) );
    }

    @Override
    public void logout( EnterpriseAuthSubject subject )
    {
        subject.logout();
    }

    // To overcome that the Shiro subject caches it's authentication status.
    // This assumes that zero authorization equals not being authenticated.
    @Override
    public boolean isAuthenticated( EnterpriseAuthSubject subject )
    {
        return subject.getShiroSubject().isAuthenticated() && subject.allowsReads();
    }

    @Override
    public AuthenticationResult authenticationResult( EnterpriseAuthSubject subject )
    {
        return subject.getAuthenticationResult();
    }

    @Override
    public void tearDown() throws Throwable
    {
        db.shutdown();
        manager.stop();
        manager.shutdown();
    }
}
