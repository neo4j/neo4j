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
package org.neo4j.kernel.diagnostics.providers;

import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.internal.diagnostics.NamedDiagnosticsProvider;
import org.neo4j.logging.Logger;

public class ConfigDiagnostics extends NamedDiagnosticsProvider
{
    private final Config config;

    public ConfigDiagnostics( Config config )
    {
        super( "DBMS config" );
        this.config = config;
    }

    @Override
    public void dump( Logger logger )
    {
        Map<String,String> configRaw = config.getRaw();
        if ( configRaw.isEmpty() )
        {
            logger.log( "No provided DBMS settings." );
            return;
        }
        logger.log( "DBMS provided settings:" );
        for ( Map.Entry<String,String> param : configRaw.entrySet() )
        {
            logger.log( "%s=%s", param.getKey(), config.obfuscateIfSecret( param ) );
        }
    }
}
