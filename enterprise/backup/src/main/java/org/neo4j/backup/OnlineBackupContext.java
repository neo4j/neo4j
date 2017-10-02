/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.backup;

import java.io.File;

import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.kernel.configuration.Config;

public class OnlineBackupContext
{
    private final OnlineBackupRequiredArguments requiredArguments;
    private final Config config;
    private final ConsistencyFlags consistencyFlags;

    public OnlineBackupContext( OnlineBackupRequiredArguments requiredArguments, Config config, ConsistencyFlags consistencyFlags )
    {
        this.requiredArguments = requiredArguments;
        this.config = config;
        this.consistencyFlags = consistencyFlags;
    }

    public OnlineBackupRequiredArguments getRequiredArguments()
    {
        return requiredArguments;
    }

    public Config getConfig()
    {
        return config;
    }

    public ConsistencyFlags getConsistencyFlags()
    {
        return consistencyFlags;
    }

    public File getResolvedLocationFromName()
    {
        return requiredArguments.getFolder().resolve( requiredArguments.getName() ).toFile();
    }
}
