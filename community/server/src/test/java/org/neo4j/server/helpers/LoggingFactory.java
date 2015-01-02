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
package org.neo4j.server.helpers;

import org.neo4j.kernel.logging.DefaultLogging;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.server.configuration.Configurator;

public abstract class LoggingFactory
{
    public abstract Logging create( Configurator configurator );

    public static LoggingFactory given( final Logging logging )
    {
        return new LoggingFactory()
        {
            @Override
            public Logging create( Configurator configurator )
            {
                return logging;
            }
        };
    }

    public static final LoggingFactory DEFAULT_LOGGING = new LoggingFactory()
    {
        @Override
        public Logging create( Configurator configurator )
        {
            return DefaultLogging.createDefaultLogging( configurator.getDatabaseTuningProperties() );
        }
    };

    public static final LoggingFactory IMPERMANENT_LOGGING = new LoggingFactory()
    {
        @Override
        public Logging create( Configurator configurator )
        {
            // Enough until the opposite is proven. This is only used by ServerBuilder,
            // which in turn is only used by tests.
            return DevNullLoggingService.DEV_NULL;
        }
    };
}
