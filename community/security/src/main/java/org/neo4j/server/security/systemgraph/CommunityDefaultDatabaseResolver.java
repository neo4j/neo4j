/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.server.security.systemgraph;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListenerAdapter;
import org.neo4j.kernel.database.DefaultDatabaseResolver;

public class CommunityDefaultDatabaseResolver extends TransactionEventListenerAdapter<Object> implements DefaultDatabaseResolver
{
    private final Config config;
    private final Supplier<GraphDatabaseService> systemDbSupplier;
    private GraphDatabaseService systemDb;

    private final AtomicReference<String> cachedDefaultDatabase = new AtomicReference<>( null );

    public CommunityDefaultDatabaseResolver( Config config, Supplier<GraphDatabaseService> systemDbSupplier )
    {
        this.config = config;
        this.systemDbSupplier = systemDbSupplier;
    }

    @Override
    public String defaultDatabase( String username )
    {
        String cachedResult = cachedDefaultDatabase.get();
        if ( cachedResult != null )
        {
            return cachedResult;
        }

        String defaultDatabase = config.get( GraphDatabaseSettings.default_database );
        try ( Transaction tx = getSystemDb().beginTx() )
        {
            Node defaultDatabaseNode = tx.findNode( Label.label( "Database" ), "default", true );
            if ( defaultDatabaseNode != null && defaultDatabaseNode.hasProperty( "name" ) )
            {
                defaultDatabase = (String) defaultDatabaseNode.getProperty( "name" );
            }
            tx.commit();
            cachedDefaultDatabase.set( defaultDatabase );
            return defaultDatabase;
        }
        catch ( NotFoundException n )
        {
            return defaultDatabase;
        }
    }

    @Override
    public void clearCache()
    {
        cachedDefaultDatabase.set( null );
    }

    private GraphDatabaseService getSystemDb()
    {
        if ( systemDb == null )
        {
            systemDb = systemDbSupplier.get();
        }
        return systemDb;
    }

    @Override
    public void afterCommit( TransactionData data, Object state, GraphDatabaseService databaseService )
    {
        clearCache();
    }
}
