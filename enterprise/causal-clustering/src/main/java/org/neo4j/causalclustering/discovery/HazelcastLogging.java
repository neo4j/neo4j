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
package org.neo4j.causalclustering.discovery;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggerFactory;

import java.util.logging.Level;

import org.neo4j.logging.LogProvider;

public class HazelcastLogging implements LoggerFactory
{
    // there is no constant in the hazelcast library for this
    private static final String HZ_LOGGING_CLASS_PROPERTY = "hazelcast.logging.class";

    private static LogProvider logProvider;
    private static Level minLevel;

    static void enable( LogProvider logProvider, Level minLevel )
    {
        HazelcastLogging.logProvider = logProvider;
        HazelcastLogging.minLevel = minLevel;

        // hazelcast only allows configuring logging through system properties
        System.setProperty( HZ_LOGGING_CLASS_PROPERTY, HazelcastLogging.class.getName() );
    }

    @Override
    public ILogger getLogger( String name )
    {
        return new HazelcastLogger( logProvider.getLog( name ), minLevel );
    }
}
