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
package org.neo4j.server.rest.web;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.server.configuration.ServerSettings;

/**
 * The mode of execution allowed for scripting inside the server.
 * <p>
 * Currently only traversal endpoints expose scripting, but this scripting feature for those endpoints can be disabled
 * by specifying the {@link #DISABLED} mode when constructing the {@link DatabaseActions}.
 */
public enum ScriptExecutionMode
{
    /**
     * Scripting is not allowed, and any attempt at invoking a script will raise an exception.
     */
    DISABLED,
    /**
     * Allow scripts to run in a sandboxed environment.
     */
    SANDBOXED,
    /**
     * Allow scripts to run without any restrictions at all. This should only be used in fully controlled environments,
     * where it can be guaranteed that no malicious scripts will make their way into the server.
     */
    UNRESTRICTED;

    /**
     * Get the execution mode that matches the given configuration.
     */
    public static ScriptExecutionMode getConfiguredMode( Config config )
    {
        if ( config.get( ServerSettings.script_enabled ) )
        {
            boolean sandboxed = config.get( ServerSettings.script_sandboxing_enabled );
            return sandboxed ? SANDBOXED : UNRESTRICTED;
        }
        return DISABLED;
    }
}
