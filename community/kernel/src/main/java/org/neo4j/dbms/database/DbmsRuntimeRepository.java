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
package org.neo4j.dbms.database;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;

import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

public abstract class DbmsRuntimeRepository
{
    public static final DbmsRuntimeVersion LATEST_VERSION = DbmsRuntimeVersion.V4_2;

    public static final Label DBMS_RUNTIME_LABEL = Label.label( "DbmsRuntime" );
    public static final String VERSION_PROPERTY = "version";

    private final DatabaseManager<?> databaseManager;

    private volatile DbmsRuntimeVersion currentVersion;

    protected DbmsRuntimeRepository( DatabaseManager<?> databaseManager )
    {
        this.databaseManager = databaseManager;
    }

    protected void fetchStateFromSystemDatabase()
    {
        var systemDatabase = getSystemDb();

        try ( var tx = systemDatabase.beginTx() )
        {
            currentVersion = tx.findNodes( DBMS_RUNTIME_LABEL ).stream()
                               .map( node -> (int) node.getProperty( VERSION_PROPERTY ) )
                               .map( DbmsRuntimeVersion::fromVersionNumber )
                               .findFirst()
                               // there always must be something in the System DB in a properly initialised DBMS,
                               // but many test don't initialise System DB
                               .orElse( LATEST_VERSION );
        }
    }

    protected GraphDatabaseService getSystemDb()
    {
        return databaseManager.getDatabaseContext( NAMED_SYSTEM_DATABASE_ID ).orElseThrow(
                () -> new RuntimeException( "Failed to get System Database" ) ).databaseFacade();
    }

    public DbmsRuntimeVersion getVersion()
    {
        if ( currentVersion == null )
        {
            synchronized ( this )
            {
                if ( currentVersion == null )
                {
                    fetchStateFromSystemDatabase();
                }
            }
        }

        return currentVersion;
    }

    /**
     * This must be used only by children and tests!!!
     */
    public void setVersion( DbmsRuntimeVersion newVersion )
    {
        currentVersion = newVersion;
    }
}
