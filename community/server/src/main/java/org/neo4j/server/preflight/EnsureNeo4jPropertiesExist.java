/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.Objects;

import org.neo4j.kernel.configuration.Config;

public class EnsureNeo4jPropertiesExist implements PreflightTask
{
    private static final String EMPTY_STRING = "";
    private boolean passed = false;
    private boolean ran = false;
    protected String failureMessage = EMPTY_STRING;
    private final Config config;

    public EnsureNeo4jPropertiesExist(Config config)
    {
        this.config = Objects.requireNonNull( config );
    }

    @Override
	public boolean run()
    {
        ran = true;

        passed = validateProperties( config );
        return passed;
    }

    protected boolean validateProperties( Config config )
    {
        // default implementation: all OK
        return true;
    }

    @Override
	public String getFailureMessage()
    {
        if ( passed )
        {
            return EMPTY_STRING;
        }

        if ( !ran )
        {
            return String.format( "%s has not been run", getClass().getName() );
        }
        else
        {
            return failureMessage;
        }
    }
}
