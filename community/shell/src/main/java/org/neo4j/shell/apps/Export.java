/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.shell.apps;

import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Service;
import org.neo4j.shell.App;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Continuation;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.impl.AbstractApp;
import org.neo4j.shell.util.json.JSONParser;

import static org.neo4j.shell.TextUtil.stripFromQuotes;

/**
 * Mimics the Bash application "export" and uses the client session
 * {@link Session} as the data container.
 */
@Service.Implementation(App.class)
public class Export extends AbstractApp
{
    @Override
    public String getDescription()
    {
        return "Sets an environment variable. Usage: export <key>=<value>\n" +
                "F.ex: export NAME=\"Mattias Persson\". Variable names have " +
                "to be valid identifiers.";
    }

    public static Pair<String, String> splitInKeyEqualsValue( String string )
            throws ShellException
    {
        int index = string.indexOf( '=' );
        if ( index == -1 )
        {
            throw new ShellException( "Invalid format <key>=<value>" );
        }

        String key = string.substring( 0, index );
        String value = string.substring( index + 1 );
        return Pair.of( key, value );
    }

    @Override
    public Continuation execute( AppCommandParser parser, Session session,
                                 Output out ) throws ShellException
    {
        Pair<String, String> keyValue = splitInKeyEqualsValue( parser.getLineWithoutApp() );
        String key = keyValue.first();
        String valueString = keyValue.other();
        if ( session.has( valueString ) )
        {
            Object value = session.get( valueString );
            session.set( key, value );
            return Continuation.INPUT_COMPLETE;
        }
        Object value = JSONParser.parse( valueString );
        value = stripFromQuotesIfString( value );
        if ( value instanceof String && value.toString().isEmpty() )
        {
            session.remove( key );
        }
        else
        {
            session.set( key, value );
        }
        return Continuation.INPUT_COMPLETE;
    }

    private Object stripFromQuotesIfString( Object value )
    {
        return value instanceof String ? stripFromQuotes( value.toString() ) : value;
    }
}
