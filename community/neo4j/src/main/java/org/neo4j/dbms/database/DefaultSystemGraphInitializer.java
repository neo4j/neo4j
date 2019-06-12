/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.dbms.database;

import java.util.function.Function;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

public class DefaultSystemGraphInitializer extends SystemGraphInitializer
{
    private final DatabaseManager<?> databaseManager;
    private final boolean isCommunity;
    private final DatabaseIdRepository databaseIdRepository;
    private final String defaultDbName;
    private final Label databaseLabel = Label.label( "Database" );
    private final Label deletedLabel = Label.label( "DeletedDatabase" );

    public DefaultSystemGraphInitializer( DatabaseManager<?> databaseManager, DatabaseIdRepository databaseIdRepository, Config config )
    {
        this( databaseManager, databaseIdRepository, config, true );
    }

    protected DefaultSystemGraphInitializer( DatabaseManager<?> databaseManager, DatabaseIdRepository databaseIdRepository, Config config, boolean isCommunity )
    {
        this.databaseManager = databaseManager;
        this.defaultDbName = config.get( GraphDatabaseSettings.default_database );
        this.isCommunity = isCommunity;
        this.databaseIdRepository = databaseIdRepository;
    }

    public void initializeSystemGraph() throws Exception
    {
        // First get a recent handle on the database representing the system graph
        GraphDatabaseFacade system = databaseManager.getDatabaseContext( databaseIdRepository.systemDatabase() ).orElseThrow(
                () -> new IllegalStateException( "No database called `" + SYSTEM_DATABASE_NAME + "` was found." ) ).databaseFacade();
        initializeSystemGraph( system );
    }

    public void initializeSystemGraph( GraphDatabaseService system ) throws Exception
    {
        // First get a recent handle on the database representing the system graph
        if ( isSystemGraphEmpty( system ) )
        {
            // If the system graph has not been initialized (typically the first time you start neo4j) we set it up by
            // creating default databases and constraints
            setupDefaultDatabasesAndConstraints( system );
        }
        else
        {
            // If the system graph exists, we make sure the default database is set correctly based on the config file settings
            // (and in community we also make sure a default database change causes the old default to be stopped)
            updateDefaultDatabase( system, isCommunity );
        }
    }

    private boolean isSystemGraphEmpty( GraphDatabaseService system )
    {
        boolean hasDatabaseNodes = false;
        try ( Transaction tx = system.beginTx() )
        {
            ResourceIterator<Node> nodes = system.findNodes( databaseLabel, "name", SYSTEM_DATABASE_NAME );
            if ( nodes.hasNext() )
            {
                hasDatabaseNodes = true;
            }
            nodes.close();
            tx.success();
        }
        return !hasDatabaseNodes;
    }

    private void setupDefaultDatabasesAndConstraints( GraphDatabaseService system ) throws InvalidArgumentsException
    {
        try ( Transaction tx = system.beginTx() )
        {
            system.schema().constraintFor( databaseLabel ).assertPropertyIsUnique( "name" ).create();
            tx.success();
        }

        newDb( system, defaultDbName, true );
        newDb( system, SYSTEM_DATABASE_NAME, false );
    }

    private void updateDefaultDatabase( GraphDatabaseService system, boolean stopOld ) throws InvalidArgumentsException
    {
        boolean defaultFound;

        try ( Transaction tx = system.beginTx() )
        {
            // A function we can apply to both :Database and :DeletedDatabase searches
            Function<ResourceIterator<Node>,Boolean> unsetOldNode = nodes ->
            {
                boolean correctDefaultFound = false;
                while ( nodes.hasNext() )
                {
                    Node oldDb = nodes.next();
                    if ( oldDb.getProperty( "name" ).equals( defaultDbName ) )
                    {
                        correctDefaultFound = true;
                    }
                    else
                    {
                        oldDb.setProperty( "default", false );
                        if ( stopOld )
                        {
                            oldDb.setProperty( "status", "offline" );
                        }
                    }
                }
                nodes.close();
                return correctDefaultFound;
            };
            // First find current default, and if it does not have the name defined as default, unset it
            defaultFound = unsetOldNode.apply( system.findNodes( databaseLabel, "default", true ) );

            // If the current default is deleted, unset it, but do not record that we found a valid default
            unsetOldNode.apply( system.findNodes( deletedLabel, "default", true ) );

            // If the old default was not the correct one, find the correct one and set the default flag
            if ( !defaultFound )
            {
                Node defaultDb = system.findNode( databaseLabel, "name", defaultDbName );
                if ( defaultDb != null )
                {
                    defaultDb.setProperty( "default", true );
                    defaultDb.setProperty( "status", "online" );
                    defaultFound = true;
                }
            }
            tx.success();
        }

        // If no database exists with the default name, create it
        if ( !defaultFound )
        {
            newDb( system, defaultDbName, true );
        }
    }

    private void newDb( GraphDatabaseService system, String dbName, boolean defaultDb ) throws InvalidArgumentsException
    {
        try ( Transaction tx = system.beginTx() )
        {
            Node node = system.createNode( databaseLabel );
            node.setProperty( "name", dbName );
            node.setProperty( "status", "online" );
            node.setProperty( "default", defaultDb );
            tx.success();
        }
        catch ( ConstraintViolationException e )
        {
            throw new InvalidArgumentsException( "The specified database '" + dbName + "' already exists." );
        }
    }
}
