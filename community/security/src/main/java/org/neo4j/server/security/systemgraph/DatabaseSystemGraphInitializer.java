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
package org.neo4j.server.security.systemgraph;

import java.util.Collections;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.cypher.result.QueryResult;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.logging.Log;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.MapUtil.map;

public class DatabaseSystemGraphInitializer
{
    protected final QueryExecutor queryExecutor;
    protected final Log log;
    private final boolean isCommunity;
    private final String defaultDbName;

    public DatabaseSystemGraphInitializer( QueryExecutor queryExecutor, Log log, Config config, boolean isCommunity )
    {
        this.queryExecutor = queryExecutor;
        this.log = log;
        this.defaultDbName = config.get( GraphDatabaseSettings.default_database );
        this.isCommunity = isCommunity;
    }

    protected void initializeSystemGraphDatabases() throws Exception
    {
        // If the system graph has not been initialized (typically the first time you start neo4j with the system graph auth provider)
        // we set it up by
        if ( isSystemGraphEmpty() )
        {
            setupDefaultDatabasesAndConstraints();
        }
        else
        {
            updateDefaultDatabase( isCommunity );
        }
    }

    public void initializeSystemGraph() throws Exception
    {
        initializeSystemGraphDatabases();
    }

    private boolean isSystemGraphEmpty()
    {
        // Execute a query to see if the system database exists
        String query = "MATCH (db:Database {name: $name}) RETURN db.name";
        Map<String,Object> params = map( "name", SYSTEM_DATABASE_NAME );

        return !queryExecutor.executeQueryWithParamCheck( query, params );
    }

    private void setupDefaultDatabasesAndConstraints() throws InvalidArgumentsException
    {
        // Ensure that multiple users, roles or databases cannot have the same name and are indexed
        final QueryResult.QueryResultVisitor<RuntimeException> resultVisitor = row -> true;
        queryExecutor.executeQuery( "CREATE CONSTRAINT ON (d:Database) ASSERT d.name IS UNIQUE", Collections.emptyMap(), resultVisitor );

        newDb( defaultDbName, true );
        newDb( SYSTEM_DATABASE_NAME, false );
    }

    private void updateDefaultDatabase( boolean stopOld ) throws InvalidArgumentsException
    {
        BasicSystemGraphOperations.assertValidDbName( defaultDbName );

        String statusUpdate = "";

        if ( stopOld )
        {
            statusUpdate = ", oldDb.status = 'offline' ";
        }

        String query = "OPTIONAL MATCH (oldDb {default: true}) " +
                       "WHERE oldDb:Database OR oldDb:DeletedDatabase " +
                       "SET oldDb.default = false " + statusUpdate +
                       "WITH oldDb " +
                       "MATCH (newDb:Database {name: $dbName}) " +
                       "SET newDb.default = true, newDb.status = 'online' " +
                       "RETURN 0";

        Map<String,Object> params = map( "dbName", defaultDbName );

        queryExecutor.executeQueryWithParamCheck( query, params, "The specified database '" + defaultDbName + "' does not exists." );
    }

    private void newDb( String dbName, boolean defaultDb ) throws InvalidArgumentsException
    {
        BasicSystemGraphOperations.assertValidDbName( dbName );

        String query = "CREATE (db:Database {name: $dbName, status: 'online', default: $defaultDb })";
        Map<String,Object> params = map( "dbName", dbName, "defaultDb", defaultDb );

        queryExecutor.executeQueryWithConstraint( query, params, "The specified database '" + dbName + "' already exists." );
    }
}
