/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
