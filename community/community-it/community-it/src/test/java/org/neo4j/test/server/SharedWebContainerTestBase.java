/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.test.server;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.util.concurrent.Callable;

import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.helpers.TestWebContainer;
import org.neo4j.server.helpers.WebContainerHelper;
import org.neo4j.test.extension.SuppressOutputExtension;

import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.test.extension.ExecutionSharedContext.SHARED_RESOURCE;
import static org.neo4j.test.extension.SuppressOutput.suppressAll;
import static org.neo4j.test.server.WebContainerHolder.release;
import static org.neo4j.test.server.WebContainerHolder.setWebContainerBuilderProperty;

@ExtendWith( SuppressOutputExtension.class )
@ResourceLock( SHARED_RESOURCE )
@ResourceLock( Resources.SYSTEM_OUT )
public class SharedWebContainerTestBase
{
    protected static TestWebContainer container()
    {
        return testWebContainer;
    }

    private static TestWebContainer testWebContainer;

    @BeforeAll
    public static void allocateServer() throws Throwable
    {
        System.setProperty( "org.neo4j.useInsecureCertificateGeneration", "true" );
        suppressAll().call( (Callable<Void>) () ->
        {
            setWebContainerBuilderProperty( GraphDatabaseSettings.cypher_hints_error.name(), TRUE );
            setWebContainerBuilderProperty( BoltConnector.enabled.name(), TRUE );
            setWebContainerBuilderProperty( BoltConnector.listen_address.name(), "localhost:0" );
            setWebContainerBuilderProperty( GraphDatabaseSettings.transaction_timeout.name(), "300s" );
            setWebContainerBuilderProperty( ServerSettings.transaction_idle_timeout.name(), "300s" );
            // Disable tracking of statement close calls, the reason being how periodic commit interacts with ResultSubscriber
            // such that some statement somehow is expected to stay open after one of the internal transactions created by periodic commit
            // has committed and will be closed later. The strict verification that statements should be closed happens to early
            // in this scenario and will always fail the internal transaction.
            setWebContainerBuilderProperty( GraphDatabaseInternalSettings.track_tx_statement_close.name(), "false" );
            // disable tracking for container because of broken periodic commit tracking
            setWebContainerBuilderProperty( GraphDatabaseSettings.memory_tracking.name(), "false" );
            testWebContainer = WebContainerHolder.allocate();
            WebContainerHelper.cleanTheDatabase( testWebContainer );
            return null;
        } );
    }

    @AfterAll
    public static void releaseServer() throws Exception
    {
        try
        {
            suppressAll().call( (Callable<Void>) () ->
            {
                release( testWebContainer );
                return null;
            } );
        }
        finally
        {
            testWebContainer = null;
        }
    }
}
