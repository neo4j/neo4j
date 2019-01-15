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
package org.neo4j.kernel.configuration;

import java.util.Map;

/**
 * Migrations of old graph database settings.
 */
public class GraphDatabaseConfigurationMigrator extends BaseConfigurationMigrator
{
    public GraphDatabaseConfigurationMigrator()
    {
        registerMigrations();
    }

    private void registerMigrations()
    {
        add( new SpecificPropertyMigration( "dbms.directories.tx_log",
                "dbms.directories.tx_log is not supported anymore. " +
                        "Please use dbms.directories.transaction.logs.root to set root directory for databases transaction logs. " +
                        "Each individual database will place its logs into a separate subdirectory under configured root." )
        {
            @Override
            public void setValueWithOldSetting( String value, Map<String,String> rawConfiguration )
            {
                // we set back into the map since our auto migration will try to use old value for automatic migration
                rawConfiguration.putIfAbsent( "dbms.directories.tx_log", value );
            }
        } );
    }
}
