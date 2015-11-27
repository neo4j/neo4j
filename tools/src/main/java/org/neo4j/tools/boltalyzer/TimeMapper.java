/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.tools.boltalyzer;


import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.neo4j.tools.boltalyzer.Fields.timestamp;

/**
 * Does conversion of timestamps
 */
public class TimeMapper
{
    public static Function<Dict, Dict> modeForName( String name )
    {
        switch( name )
        {
        case "epoch":
            // This is the format we get from the PCAP files, so no need to convert anything
            return (p) -> p;
        case "session-delta":
            // Each packet timestamp is shown as a delta of the last packet seen in the same session - eg. how many usec since the last message passed
            // before this message was sent?
            return new SessionDelta( Fields.timestamp, Fields.connectionKey );
        case "global-incremental":
        case "gi":
            // First seen packet is timestamp 0, timestamps after that are micro seconds since the first packet
            return new GlobalIncremental( Fields.timestamp );
        default:
            throw new RuntimeException( "No time mapping mechanism named `" + name + "` available. Please choose 'global-incremental' or 'session-delta'." );
        }
    }

    public static Function<Dict, Dict> unitForName( String unit )
    {
        switch( unit )
        {
        case "us":
            // This is the unit we get from the PCAP files, so no need to convert anything
            return (p) -> p;
        case "ms":
            // Each packet timestamp is shown as a delta of the last packet seen in the same session - eg. how many usec since the last message passed
            // before this message was sent?
            return (p) -> {
                p.put( timestamp, p.get( timestamp ) / 1000 );
                return p;
            };
        default:
            throw new RuntimeException( "Don't know how to convert to `" + unit + "`." );
        }
    }

    private static class GlobalIncremental implements Function<Dict, Dict>
    {
        private final Field<Long> timestampKey;
        private long firstTimestamp = -1;

        public GlobalIncremental( Field<Long> timestampKey )
        {
            this.timestampKey = timestampKey;
        }

        @Override
        public Dict apply( Dict packet )
        {
            if( firstTimestamp == -1 )
            {
                firstTimestamp = packet.get( timestampKey );
            }
            packet.put( timestampKey, packet.get( timestampKey ) - firstTimestamp );
            return packet;
        }
    }

    private static class SessionDelta implements Function<Dict, Dict>
    {
        /** Track the last timestamp of any message going either way for a given connection */
        private final Map<Object, Long> lastTimestampInConnection = new HashMap<>();
        private final Field<Long> timestampKey;
        private final Field<String> groupingKey;

        public SessionDelta( Field<Long> timestampKey, Field<String> groupingKey )
        {
            this.timestampKey = timestampKey;
            this.groupingKey = groupingKey;
        }

        @Override
        public Dict apply( Dict packet )
        {
            Long timestamp = packet.get( timestampKey );

            Long lastSeen = lastTimestampInConnection.getOrDefault( packet.get( groupingKey ), timestamp );
            lastTimestampInConnection.put( packet.get( groupingKey ), timestamp );

            packet.put( timestampKey, timestamp - lastSeen );
            return packet;
        }
    }


}