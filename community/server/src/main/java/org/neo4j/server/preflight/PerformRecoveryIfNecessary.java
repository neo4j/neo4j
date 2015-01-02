/**
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

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;

import org.apache.commons.configuration.Configuration;

import org.neo4j.kernel.impl.recovery.StoreRecoverer;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.server.configuration.Configurator;

public class PerformRecoveryIfNecessary implements PreflightTask
{
    private final String failureMessage = "Unable to recover database";
    private final Configuration config;
    private final PrintStream out;
    private final Map<String, String> dbConfig;
    private final ConsoleLogger log;

    public PerformRecoveryIfNecessary( Configuration serverConfig, Map<String, String> dbConfig, PrintStream out,
            Logging logging )
    {
        this.config = serverConfig;
        this.dbConfig = dbConfig;
        this.out = out;
        this.log = logging.getConsoleLog( getClass() );
    }

    @Override
    public boolean run()
    {
        try
        {
            File dbLocation = new File( config.getString( Configurator.DATABASE_LOCATION_PROPERTY_KEY ) );
            if ( dbLocation.exists() )
            {
                StoreRecoverer recoverer = new StoreRecoverer();
                if ( recoverer.recoveryNeededAt( dbLocation, dbConfig ) )
                {
                    out.println( "Detected incorrectly shut down database, performing recovery.." );
                    recoverer.recover( dbLocation, dbConfig );
                }
            }
            return true;
        }
        catch ( IOException e )
        {
            log.error( "Recovery startup task failed.", e );
            return false;
        }
    }

    @Override
    public String getFailureMessage()
    {
        return failureMessage;
    }
}
