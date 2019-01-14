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
package org.neo4j.shell.impl;

import java.rmi.RemoteException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.internal.Version;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellServer;

/**
 * Can replace the prompt string (PS1) with common Bash variable interpretation,
 * f.ex. "\h [\t] \W $ " would result in "shell [10:05:30] 1243 $"
 */
public class BashVariableInterpreter
{
    private static final Map<String, Replacer> STATIC_REPLACERS = new HashMap<>();

    static
    {
        STATIC_REPLACERS.put( "d", new DateReplacer( "EEE MMM dd" ) );
        STATIC_REPLACERS.put( "h", new HostReplacer() );
        STATIC_REPLACERS.put( "H", new HostReplacer() );
        STATIC_REPLACERS.put( "s", new HostReplacer() );
        STATIC_REPLACERS.put( "t", new DateReplacer( "HH:mm:ss" ) );
        STATIC_REPLACERS.put( "T", new DateReplacer( "KK:mm:ss" ) );
        STATIC_REPLACERS.put( "@", new DateReplacer( "KK:mm aa" ) );
        STATIC_REPLACERS.put( "A", new DateReplacer( "HH:mm" ) );
        STATIC_REPLACERS.put( "u", new StaticReplacer( "user" ) );
        STATIC_REPLACERS.put( "v", new StaticReplacer( Version.getNeo4jVersion() ) );
        STATIC_REPLACERS.put( "V", new StaticReplacer( Version.getKernelVersion() ) );
    }

    private final Map<String, Replacer> localReplacers = new HashMap<>();

    /**
     * Adds a customized replacer for a certain variable.
     * @param key the variable key, f.ex. "t".
     * @param replacer the replacer which gives a replacement for the variable.
     */
    public void addReplacer( String key, Replacer replacer )
    {
        localReplacers.put( key, replacer );
    }

    /**
     * Interprets a string with variables in it and replaces those variables
     * with values from replacers, see {@link Replacer}.
     * @param string the string to interpret.
     * @param server the server which runs the interpretation.
     * @param session the session (or environment) of the interpretation.
     * @return the interpreted string.
     */
    public String interpret( String string, ShellServer server, Session session )
    {
        string = replace( string, server, session, localReplacers );
        string = replace( string, server, session, STATIC_REPLACERS );
        return string;
    }

    private String replace( String string, ShellServer server, Session session, Map<String, Replacer> replacers )
    {
        for ( Map.Entry<String, Replacer> replacer : replacers.entrySet() )
        {
            String value = replacer.getValue().getReplacement( server, session );
            string = string.replaceAll( "\\\\" + replacer.getKey(), value );
        }
        return string;
    }

    /**
     * A replacer which can return a string to replace a variable.
     */
    public interface Replacer
    {
        /**
         * Returns a string to replace something else.
         * @param server the server which runs the interpretation.
         * @param session the environment of the interpretation.
         * @return the replacement.
         */
        String getReplacement( ShellServer server, Session session );
    }

    /**
     * A {@link Replacer} which gets instantiated with a string representing the
     * replacement, which means that the value returned from
     * {@link #getReplacement(ShellServer, Session)} is always the same.
     */
    public static class StaticReplacer implements Replacer
    {
        private final String value;

        /**
         * @param value the value to return from
         * {@link #getReplacement(ShellServer, Session)}.
         */
        StaticReplacer( String value )
        {
            this.value = value;
        }

        @Override
        public String getReplacement( ShellServer server, Session session )
        {
            return this.value;
        }
    }

    /**
     * A {@link Replacer} which returns a date string in a certain format.
     */
    public static class DateReplacer implements Replacer
    {
        private final DateFormat format;

        /**
         * @param format the date format, see {@link SimpleDateFormat}.
         */
        DateReplacer( String format )
        {
            this.format = new SimpleDateFormat( format );
        }

        @Override
        public String getReplacement( ShellServer server, Session session )
        {
            return format.format( new Date() );
        }
    }

    /**
     * Returns the name of the server (or "host").
     */
    public static class HostReplacer implements Replacer
    {
        @Override
        public String getReplacement( ShellServer server, Session session )
        {
            try
            {
                return server.getName();
            }
            catch ( RemoteException e )
            {
                return "";
            }
        }
    }
}
