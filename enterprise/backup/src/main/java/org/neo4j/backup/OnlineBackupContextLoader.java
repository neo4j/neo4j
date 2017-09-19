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

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.kernel.configuration.Config;

/**
 * A helper class that performs processing of command line arguments and configuration and groups the result of this processing for easier handling
 */
public class OnlineBackupContextLoader
{
    private final BackupCommandArgumentHandler backupCommandArgumentHandler;
    private final OnlineBackupCommandConfigLoader onlineBackupCommandConfigLoader;

    public OnlineBackupContextLoader( BackupCommandArgumentHandler backupCommandArgumentHandler,
            OnlineBackupCommandConfigLoader onlineBackupCommandConfigLoader )
    {
        this.backupCommandArgumentHandler = backupCommandArgumentHandler;
        this.onlineBackupCommandConfigLoader = onlineBackupCommandConfigLoader;
    }

    /**
     * Process command line arguments, establish if they are valid and group them conveniently into a {@link OnlineBackupContext}
     * @param commandlineArgs Command line arguments split into an array by space
     * @return the resolved configuration and arguments used in controlling the behaviour of the backup tool
     */
    public OnlineBackupContext fromCommandLineArguments( String[] commandlineArgs )
    {
        try
        {
            OnlineBackupRequiredArguments requiredArguments = backupCommandArgumentHandler.establishRequiredArguments( commandlineArgs );
            Config config = onlineBackupCommandConfigLoader.loadConfig( requiredArguments.getAdditionalConfig() );
            ConsistencyFlags consistencyFlags = backupCommandArgumentHandler.readFlagsFromArgumentsOrDefaultToConfig( config );

            return new OnlineBackupContext( requiredArguments, config, consistencyFlags );
        }
        catch ( IncorrectUsage | CommandFailed e )
        {
            throw new IllegalArgumentException( "Something went wrong when evaluating input", e );
        }
    }
}
