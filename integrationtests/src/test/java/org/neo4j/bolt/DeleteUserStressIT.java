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
package org.neo4j.bolt;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.harness.junit.Neo4jRule;

import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.server.configuration.ServerSettings;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.neo4j.driver.v1.AuthTokens.basic;

public class DeleteUserStressIT
{
    @Rule
    public Neo4jRule db = new Neo4jRule()
            .withConfig( GraphDatabaseSettings.auth_enabled, "true" )
            .withConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
            .withConfig( ServerSettings.script_enabled.name(), Settings.TRUE );

    private Driver adminDriver;
    private final Set<Throwable> errors = ConcurrentHashMap.newKeySet();

    @Before
    public void setup()
    {
        adminDriver = GraphDatabase.driver( db.boltURI(), basic( "neo4j", "neo4j" ) );
        try ( Session session = adminDriver.session();
              Transaction tx = session.beginTransaction() )
        {
            tx.run( "CALL dbms.changePassword('abc')" ).consume();
            tx.success();
        }
        adminDriver.close();
        adminDriver = GraphDatabase.driver( db.boltURI(), basic( "neo4j", "abc" ) );
    }

    @Test
    public void shouldRun() throws InterruptedException
    {
        ExecutorService service = Executors.newFixedThreadPool( 3 );
        service.submit( createUserWork );
        service.submit( deleteUserWork );
        service.submit( transactionWork );

        service.awaitTermination( 30, TimeUnit.SECONDS );

        String msg = String.join( System.lineSeparator(),
                errors.stream().map( Throwable::getMessage ).collect( Collectors.toList() ) );
        assertThat( msg, errors, empty() );
    }

    @SuppressWarnings( "InfiniteLoopStatement" )
    private final Runnable transactionWork = () ->
    {

        for (; ; )
        {
            try ( Driver driver = GraphDatabase.driver( db.boltURI(), basic( "pontus", "sutnop" ) ) )
            {

                try ( Session session = driver.session();
                      Transaction tx = session.beginTransaction() )
                {
                    tx.run( "UNWIND range(1, 100000) AS n RETURN n" ).consume();
                    tx.success();
                }
            }
            catch ( ClientException e )
            {
                if ( !e.getMessage().equals( "The client is unauthorized due to authentication failure." ) )
                {
                    errors.add( e );
                }
            }
        }
    };

    @SuppressWarnings( "InfiniteLoopStatement" )
    private final Runnable deleteUserWork = () ->
    {

        for (; ; )
        {
            try ( Session session = adminDriver.session();
                  Transaction tx = session.beginTransaction() )
            {
                tx.run( "CALL dbms.security.deleteUser('pontus')" ).consume();
                tx.success();
            }
            catch ( ClientException e )
            {
                if ( !e.getMessage().equals( "User 'pontus' does not exist." ) )
                {
                    errors.add( e );
                }
            }
        }
    };

    @SuppressWarnings( "InfiniteLoopStatement" )
    private final Runnable createUserWork = () ->
    {
        for (; ; )
        {
            try ( Session session = adminDriver.session();
                  Transaction tx = session.beginTransaction() )
            {
                tx.run( "CALL dbms.security.createUser('pontus', 'sutnop', false)" ).consume();
                tx.success();
            }
            catch ( ClientException e )
            {
                if ( !e.getMessage().equals( "The specified user 'pontus' already exists." ) )
                {
                    errors.add( e );
                }
            }
        }
    };
}
