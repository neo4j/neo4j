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
package org.neo4j.dbms.database;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.util.Preconditions;
import org.neo4j.util.VisibleForTesting;

import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

public abstract class DbmsRuntimeRepository
{
    public static final DbmsRuntimeVersion LATEST_VERSION = DbmsRuntimeVersion.V4_2;
    public static final DbmsRuntimeVersion PREVIOUS_VERSION = DbmsRuntimeVersion.V4_1;

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

        try ( var tx = systemDatabase.beginTx();
                var nodes = tx.findNodes( DBMS_RUNTIME_LABEL ) )
        {
            if ( nodes.hasNext() )
            {
                currentVersion = DbmsRuntimeVersion.fromVersionNumber( (int) nodes.next().getProperty( VERSION_PROPERTY ) );
                Preconditions.checkState( !nodes.hasNext(), "More than one dbms-runtime node in system database" );
            }
            else
            {
                currentVersion = getFallbackVersion();
            }
        }
    }

    /**
     * A fallback DBMS runtime version used when there is nothing in System database.
     */
    protected abstract DbmsRuntimeVersion getFallbackVersion();

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
    @VisibleForTesting
    public void setVersion( DbmsRuntimeVersion newVersion )
    {
        currentVersion = newVersion;
    }
}
