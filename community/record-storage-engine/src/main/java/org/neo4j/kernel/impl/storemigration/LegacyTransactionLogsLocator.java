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
import java.util.Optional;

import org.neo4j.kernel.configuration.Config;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.logical_logs_location;

public class LegacyTransactionLogsLocator
{
    private final Config config;
    private final File databaseDirectory;

    public LegacyTransactionLogsLocator( Config config, File databaseDirectory )
    {
        this.config = config;
        this.databaseDirectory = databaseDirectory;
    }

    public File getTransactionLogsDirectory()
    {
        Optional<String> customOldLogsLocation = config.getRaw( logical_logs_location.name() );
        return customOldLogsLocation.map( value -> isNotBlank( value ) ? config.get( logical_logs_location ) : null ).orElse( databaseDirectory );
    }
}
