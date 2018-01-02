/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.tools.console.input;

import io.airlift.airline.Cli;
import io.airlift.airline.Help;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.StringTokenizer;

import org.neo4j.kernel.impl.util.Listener;

import static org.neo4j.helpers.ArrayUtil.join;

public class ConsoleUtil
{
    public static final Listener<PrintStream> NO_PROMPT = new Listener<PrintStream>()
    {
        @Override
        public void receive( PrintStream out )
        {   // Do nothing
        }
    };

    public static final Listener<PrintStream> staticPrompt( final String prompt )
    {
        return new Listener<PrintStream>()
        {
            @Override
            public void receive( PrintStream out )
            {
                out.print( prompt );
            }
        };
    }

    public static final OutputStream NULL_OUTPUT_STREAM = new OutputStream()
    {
        @Override
        public void close(){}
        @Override
        public void flush(){}
        @Override
        public void write(byte[]b){}
        @Override
        public void write(byte[]b,int i,int l){}
        @Override
        public void write(int b){}
    };

    public static final PrintStream NULL_PRINT_STREAM = new PrintStream( NULL_OUTPUT_STREAM );

    public static String[] tokenizeStringWithQuotes( String string )
    {
        ArrayList<String> result = new ArrayList<>();
        string = string.trim();
        boolean inside = string.startsWith( "\"" );
        StringTokenizer quoteTokenizer = new StringTokenizer( string, "\"" );
        while ( quoteTokenizer.hasMoreTokens() )
        {
            String token = quoteTokenizer.nextToken();
            token = token.trim();
            if ( token.length() == 0 )
            {
                // Skip it
            }
            else if ( inside )
            {
                // Don't split
                result.add( token );
            }
            else
            {
                splitAndKeepEscapedSpaces( token, false, result );
            }
            inside = !inside;
        }
        return result.toArray( new String[result.size()] );
    }

    private static void splitAndKeepEscapedSpaces( String string, boolean preserveEscapes, Collection<String> into )
    {
        StringBuilder current = new StringBuilder();
        for ( int i = 0; i < string.length(); i++ )
        {
            char ch = string.charAt( i );
            if ( ch == ' ' )
            {
                boolean isGluedSpace = i > 0 && string.charAt( i-1 ) == '\\';
                if ( !isGluedSpace )
                {
                    into.add( current.toString() );
                    current = new StringBuilder();
                    continue;
                }
            }

            if ( preserveEscapes || ch != '\\' )
            {
                current.append( ch );
            }
        }
        if ( current.length() > 0 )
        {
            into.add( current.toString() );
        }
    }

    public static InputStream oneCommand( String... args )
    {
        String string = join( args, " " );
        return new ByteArrayInputStream( string.getBytes() );
    }

    public static String airlineHelp( Cli<?> cli )
    {
        StringBuilder builder = new StringBuilder();
        Help.help( cli.getMetadata(), Collections.<String>emptyList(), builder );
        return builder.toString();
    }

    private ConsoleUtil()
    {
    }
}
