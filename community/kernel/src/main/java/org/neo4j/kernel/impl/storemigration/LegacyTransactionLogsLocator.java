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
package org.neo4j.kernel.impl.storemigration;

import java.io.File;

import org.neo4j.configuration.Config;
import org.neo4j.io.layout.DatabaseLayout;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.logical_logs_location;

public class LegacyTransactionLogsLocator
{
    public static final String LEGACY_TX_LOGS_LOCATION_SETTING = "dbms.directories.tx_log";
    private final Config config;
    private final DatabaseLayout databaseLayout;

    public LegacyTransactionLogsLocator( Config config, DatabaseLayout databaseLayout )
    {
        this.config = config;
        this.databaseLayout = databaseLayout;
    }

    public File getTransactionLogsDirectory()
    {
        File databaseDirectory = databaseLayout.databaseDirectory();
        if ( databaseLayout.getDatabaseName().equals( SYSTEM_DATABASE_NAME ) )
        {
            return databaseDirectory;
        }
        config.setIfNotSet( logical_logs_location, databaseDirectory.toPath() );
        return config.get( logical_logs_location ).toFile();
    }
}
