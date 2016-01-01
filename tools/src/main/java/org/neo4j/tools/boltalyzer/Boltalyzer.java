/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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


import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import org.neo4j.helpers.Args;

import static org.neo4j.tools.boltalyzer.Fields.connectionKey;
import static org.neo4j.tools.boltalyzer.Fields.description;
import static org.neo4j.tools.boltalyzer.Fields.dstPort;
import static org.neo4j.tools.boltalyzer.Fields.logicalSource;
import static org.neo4j.tools.boltalyzer.Fields.payload;
import static org.neo4j.tools.boltalyzer.Fields.session;
import static org.neo4j.tools.boltalyzer.Fields.timestamp;
import static org.neo4j.tools.boltalyzer.TimeMapper.modeForName;
import static org.neo4j.tools.boltalyzer.TimeMapper.unitForName;

public class Boltalyzer
{
    public static void main(String ... argv) throws IOException
    {
        if ( argv.length == 0 || argv[0].equals( "-h" ) || argv[0].equals( "--help" ) )
        {
            System.out.println(
                    "USAGE\n" +
                    "  boltalyzer [options] <PCAP_FILE>\n" +
                    "\n" +
                    "OPTIONS\n" +
                    "  --timemode [epoch | global-incremental | session-delta]  (default: session-delta)\n" +
                    "  --timeunit [us | ms]  (default: us)\n" +
                    "  --serverport <port>  (default: 7687)\n" +
                    "  --session [<n> | all]  \n" +
                    "      Filter which sessions to show, session id is incrementally determined in order of sessions appearing in the data dump.  (default: all)\n" +
                    "  --skip <n>  Skip n packets before starting output    (default: 0)\n"
            );
            System.exit( 0 );
        }

        Args args = Args.parse( argv );

        try ( FileInputStream pcap = new FileInputStream( args.orphans().get( 0 ) ) )
        {
            // Implementation note: since this is in a sideline tool, I'm using it to play a bit.
            // The approach here uses maps instead of classes (a la http://confreaks.tv/videos/railsconf2012-keynote-simplicity-matters )
            // These maps are passed through the pipeline defined below, and are mutated as they pass through it.
            //
            // The idea here being that we will want a set of different pipelines for analyzing the stream from different perspectives.
            // Rather than having a dozen or so in-between-state-classes with specialized functions acting on them, we have generalized
            // functions acting on maps. This should put us in a situation where we can construct different pipelines from a common set of
            // pipeline functions.

            new PCAPParser()

                    // So, parse out a stream of packets from the pcap file
                    .parse( pcap )

                            // Modify the timestamps on those packets to fit the users chosen time mode
                    .map( modeForName( args.get( "timemode", "session-delta" ) ) )

                            // And convert the timestamps to whatever units the user wants
                    .map( unitForName( args.get( "timeunit", "us" ) ) )

                            // Decorate each packet with semantic information about what the actual bolt messages were,
                            // what the logical session and logical source of the message was
                    .map( new AddBoltDescription( args.getNumber( "serverport", 7687 ).intValue() ) )

                            // Now we can skip things (currently the step above needs to see all packets to maintain message framing alignment, so
                            // we can't skip until after the step above)
                    .skip( args.getNumber( "skip", 0 ).intValue() )

                            // Filter out to only look sessions the user cares about
                    .filter( sessionFilter( args.get( "session", "all" ) ) )

                            // And convert the result to a readable output string
                    .map( ( p ) -> String.format( "%010d %s %s%s", p.get( timestamp ), p.get( session ).name(), p.get( logicalSource ), p.get( description ) ) )
                    .forEach( System.out::println );
        }
    }

    private static Predicate<Dict> sessionFilter( String name )
    {
        switch( name )
        {
        case "all": return (p) -> true;
        default:
            long sessionId = Long.parseLong( name );
            return (p) -> p.get( session ).id() == sessionId;
        }
    }

    public static class SessionRepository
    {
        private final Map<Object,AnalyzedSession> openSessions = new HashMap<>();
        private int sessionCount = 0;

        public AnalyzedSession session( Object connectionKey )
        {
            AnalyzedSession session = openSessions.get( connectionKey );
            if( session == null )
            {
                int sid = sessionCount++;
                session = new AnalyzedSession(  String.format("session-%03d", sid ), sid );
                openSessions.put( connectionKey, session );
            }
            return session;
        }
    }

    /** Adds a description of the messages in each packet, plus semantic info about who is sending it and attaches a session object to it */
    public static class AddBoltDescription implements Function<Dict, Dict>
    {
        private final SessionRepository sessions = new SessionRepository();
        private final int serverPort;

        public AddBoltDescription( int serverPort )
        {
            this.serverPort = serverPort;
        }

        @Override
        public Dict apply( Dict packet )
        {
            try
            {
                if ( packet.get( dstPort ) == serverPort )
                {
                    AnalyzedSession sess = sessions.session( packet.get( connectionKey ) );
                    packet.put( description, sess.describeClientPayload( packet.get( payload ) ) );
                    packet.put( session, sess );
                    packet.put( logicalSource, "Client" );
                }
                else
                {
                    AnalyzedSession sess = sessions.session( packet.get( connectionKey ) );
                    packet.put( description, sess.describeServerPayload( packet.get( payload ) ) );
                    packet.put( session, sess );
                    packet.put( logicalSource, "Server" );
                }
                return packet;
            }
            catch( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
    }
}