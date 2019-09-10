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

import java.util.UUID;
import java.util.function.Function;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.NormalizedDatabaseName;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.dbms.database.SystemGraphDbmsModel.DATABASE_DEFAULT_PROPERTY;
import static org.neo4j.dbms.database.SystemGraphDbmsModel.DATABASE_LABEL;
import static org.neo4j.dbms.database.SystemGraphDbmsModel.DATABASE_NAME_PROPERTY;
import static org.neo4j.dbms.database.SystemGraphDbmsModel.DATABASE_STATUS_PROPERTY;
import static org.neo4j.dbms.database.SystemGraphDbmsModel.DATABASE_UUID_PROPERTY;
import static org.neo4j.dbms.database.SystemGraphDbmsModel.DELETED_DATABASE_LABEL;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

public class DefaultSystemGraphInitializer extends SystemGraphInitializer
{
    private final DatabaseManager<?> databaseManager;
    private final boolean isCommunity;
    private final NormalizedDatabaseName defaultDbName;

    public DefaultSystemGraphInitializer( DatabaseManager<?> databaseManager, Config config )
    {
        this( databaseManager, config, true );
    }

    protected DefaultSystemGraphInitializer( DatabaseManager<?> databaseManager, Config config, boolean isCommunity )
    {
        this.databaseManager = databaseManager;
        this.defaultDbName = new NormalizedDatabaseName( config.get( GraphDatabaseSettings.default_database ) );
        this.isCommunity = isCommunity;
    }

    @Override
    public void initializeSystemGraph() throws Exception
    {
        // First get a recent handle on the database representing the system graph
        GraphDatabaseFacade system = databaseManager.getDatabaseContext( NAMED_SYSTEM_DATABASE_ID ).orElseThrow(
                () -> new IllegalStateException( "No database called `" + SYSTEM_DATABASE_NAME + "` was found." ) ).databaseFacade();
        initializeSystemGraph( system );
    }

    @Override
    public void initializeSystemGraph( GraphDatabaseService system ) throws Exception
    {
        // First get a recent handle on the database representing the system graph
        if ( isSystemGraphEmpty( system ) )
        {
            // If the system graph has not been initialized (typically the first time you start neo4j) we set it up by
            // creating default databases and constraints
            setupDefaultDatabasesAndConstraints( system );
            manageDatabases( system, false );
        }
        else
        {
            // If the system graph exists, we make sure the default database is set correctly based on the config file settings
            // (and in community we also make sure a default database change causes the old default to be stopped)
            updateDefaultDatabase( system, isCommunity );
            manageDatabases( system, true );
        }
    }

    protected void manageDatabases( GraphDatabaseService system, boolean update )
    {

    }

    private boolean isSystemGraphEmpty( GraphDatabaseService system )
    {
        boolean hasDatabaseNodes = false;
        try ( Transaction tx = system.beginTx() )
        {
            ResourceIterator<Node> nodes = tx.findNodes( DATABASE_LABEL, DATABASE_NAME_PROPERTY, SYSTEM_DATABASE_NAME );
            if ( nodes.hasNext() )
            {
                hasDatabaseNodes = true;
            }
            nodes.close();
            tx.commit();
        }
        return !hasDatabaseNodes;
    }

    private void setupDefaultDatabasesAndConstraints( GraphDatabaseService system ) throws InvalidArgumentsException
    {
        try ( Transaction tx = system.beginTx() )
        {
            tx.schema().constraintFor( DATABASE_LABEL ).assertPropertyIsUnique( DATABASE_NAME_PROPERTY ).create();
            tx.commit();
        }

        newDb( system, defaultDbName, true, UUID.randomUUID() );
        newDb( system, new NormalizedDatabaseName( SYSTEM_DATABASE_NAME ), false, NAMED_SYSTEM_DATABASE_ID.databaseId().uuid() );
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
                    if ( oldDb.getProperty( DATABASE_NAME_PROPERTY ).equals( defaultDbName.name() ) )
                    {
                        correctDefaultFound = true;
                    }
                    else
                    {
                        oldDb.setProperty( DATABASE_DEFAULT_PROPERTY, false );
                        if ( stopOld )
                        {
                            oldDb.setProperty( DATABASE_STATUS_PROPERTY, "offline" );
                        }
                    }
                }
                nodes.close();
                return correctDefaultFound;
            };
            // First find current default, and if it does not have the name defined as default, unset it
            defaultFound = unsetOldNode.apply( tx.findNodes( DATABASE_LABEL, DATABASE_DEFAULT_PROPERTY, true ) );

            // If the current default is deleted, unset it, but do not record that we found a valid default
            unsetOldNode.apply( tx.findNodes( DELETED_DATABASE_LABEL, DATABASE_DEFAULT_PROPERTY, true ) );

            // If the old default was not the correct one, find the correct one and set the default flag
            if ( !defaultFound )
            {
                Node defaultDb = tx.findNode( DATABASE_LABEL, DATABASE_NAME_PROPERTY, defaultDbName.name() );
                if ( defaultDb != null )
                {
                    defaultDb.setProperty( DATABASE_DEFAULT_PROPERTY, true );
                    defaultDb.setProperty( DATABASE_STATUS_PROPERTY, "online" );
                    defaultFound = true;
                }
            }
            tx.commit();
        }

        // If no database exists with the default name, create it
        if ( !defaultFound )
        {
            newDb( system, defaultDbName, true, UUID.randomUUID() );
        }
    }

    private void newDb( GraphDatabaseService system, NormalizedDatabaseName databaseName, boolean defaultDb, UUID uuid ) throws InvalidArgumentsException
    {
        try ( Transaction tx = system.beginTx() )
        {
            Node node = tx.createNode( DATABASE_LABEL );
            node.setProperty( DATABASE_NAME_PROPERTY, databaseName.name() );
            node.setProperty( DATABASE_UUID_PROPERTY, uuid.toString() );
            node.setProperty( DATABASE_STATUS_PROPERTY, "online" );
            node.setProperty( DATABASE_DEFAULT_PROPERTY, defaultDb );
            tx.commit();
        }
        catch ( ConstraintViolationException e )
        {
            throw new InvalidArgumentsException( "The specified database '" + databaseName + "' already exists." );
        }
    }
}
