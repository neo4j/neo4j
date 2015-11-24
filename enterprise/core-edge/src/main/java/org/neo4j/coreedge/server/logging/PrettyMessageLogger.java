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
package org.neo4j.coreedge.server.logging;

import java.io.PrintWriter;

import org.neo4j.coreedge.raft.RaftMessages;

public class PrettyMessageLogger<MEMBER> implements MessageLogger<MEMBER>
{
    private final PrintWriter printWriter;

    public PrettyMessageLogger( PrintWriter printWriter )
    {
        this.printWriter = printWriter;
    }

    public static String wrapString( String string, int charWrap )
    {
        int lastBreak = 0;
        int nextBreak = charWrap;
        if ( string.length() > charWrap )
        {
            String setString = "";
            do
            {
                while ( string.charAt( nextBreak ) != ' ' && nextBreak > lastBreak )
                {
                    nextBreak--;
                }
                if ( nextBreak == lastBreak )
                {
                    nextBreak = lastBreak + charWrap;
                }
                setString += string.substring( lastBreak, nextBreak ).trim() + "\\n";
                lastBreak = nextBreak;
                nextBreak += charWrap;

            } while ( nextBreak < string.length() );
            setString += string.substring( lastBreak ).trim();
            return setString;
        }
        else
        {
            return string;
        }
    }

    @Override
    public void log( MEMBER from, MEMBER to, Object... messages )
    {
        for ( Object message : messages )
        {
            log( from, to, message );
        }
    }


    private void log( MEMBER from, MEMBER to, Object message )
    {
        String prettyFrom = pretty( from );
        String prettyTo = pretty( to );
        if ( message instanceof RaftMessages.NewEntry.Request && from.equals( to ) )
        {
            prettyFrom = "Client";
        }

        String note = String.format( "Note over %s,%s: %s", prettyFrom, prettyTo, wrapString( String.valueOf( message ),
                100 ) );


        printWriter.println( note );
        printWriter.println( prettyFrom + "->" + prettyTo + ": " + message.getClass().getSimpleName() );
        printWriter.flush();
    }

    @Override
    public void log( MEMBER to, Object message )
    {
        // Do nothing?
    }

    private String pretty( MEMBER from )
    {
        return String.valueOf( from ).replace( ":", "/" );
    }
}
