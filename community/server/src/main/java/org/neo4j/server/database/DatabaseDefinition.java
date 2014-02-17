/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.server.database;

import java.io.File;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;

/** Metadata about a database that runs in the server. */
public class DatabaseDefinition
{
    private final String key;
    private final String provider;
    private final DatabaseHosting.Mode mode;
    private final Config config;

    public DatabaseDefinition( String key, String provider, DatabaseHosting.Mode mode, Config config )
    {
        this.key = key;
        this.provider = provider;
        this.mode = mode;
        this.config = config;
    }

    public String key()
    {
        return key;
    }

    public String provider()
    {
        return provider;
    }

    public Config config()
    {
        return config;
    }

    public DatabaseHosting.Mode mode()
    {
        return mode;
    }

    public File path()
    {
        return config.get( GraphDatabaseSettings.store_dir);
    }
}
