/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.preflight;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * These are tasks that are run on server startup that may take a long time
 * to execute, such as recovery, upgrades and so on.
 *
 * This implementation still needs some work, because of some of the refactoring
 * done regarding the NeoServer. Specifically, some of these tasks verify that
 * properties files exist and are valid. Other preflight tasks we might want to
 * add could be auto-generating config files if they don't exist and creating required
 * directories.
 *
 * All of these (including auto-generating neo4j.properties and creating directories)
 * except validating and potentially generating neo4j-server.properties depend on having
 * the server configuration available. Eg. we can't both ensure that file exists within these
 * tests, while at the same time depending on that file existing.
 *
 * The validation is not a problem, because we will refactor the server config to use the
 * new configuration system from the kernel, which automatically validates itself.
 *
 * Ensuring the config file exists (and potentially auto-generating it) is a problem.
 * Either this need to be split into tasks that have dependencies, and tasks that don't.
 *
 * Although, it seems it is only this one edge case, so perhaps accepting that and adding
 * code to the bootstrapper to ensure the config file exists is acceptable.
 */
public class PreFlightTasks
{
    private final PreflightTask[] tasks;
    private final Log log;

    private PreflightTask failedTask = null;

    public PreFlightTasks( LogProvider logProvider, PreflightTask... tasks )
    {
        this.tasks = tasks;
        this.log = logProvider.getLog( getClass() );
    }

    public boolean run()
    {
        if ( tasks == null || tasks.length < 1 )
        {
            return true;
        }

        for ( PreflightTask r : tasks )
        {
            if ( !r.run() )
            {
                log.error( r.getFailureMessage() );
                failedTask = r;
                return false;
            }
        }

        return true;
    }

    public PreflightTask failedTask()
    {
        return failedTask;
    }
}
