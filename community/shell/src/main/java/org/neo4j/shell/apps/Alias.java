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
import org.neo4j.shell.impl.AbstractApp;

@Service.Implementation(App.class)
public class Alias extends AbstractApp
{

    @Override
    public String getDescription()
    {
        return "Adds an alias so that it can be used later as a command.\n" +
                "Usage: alias <key>=<value>";
    }

    public Continuation execute( AppCommandParser parser, Session session,
                                 Output out ) throws Exception
    {
        String line = parser.getLineWithoutApp();
        if ( line.trim().length() == 0 )
        {
            printAllAliases( session, out );
            return Continuation.INPUT_COMPLETE;
        }

        Pair<String, String> keyValue = Export.splitInKeyEqualsValue( line );
        String key = keyValue.first();
        String value = keyValue.other();
        if ( value == null || value.trim().length() == 0 )
        {
            session.removeAlias( key );
        }
        else
        {
            session.setAlias( key, value );
        }
        return Continuation.INPUT_COMPLETE;
    }

    private void printAllAliases( Session session, Output out )
            throws Exception
    {
        for ( String key : session.getAliasKeys() )
        {
            out.println( "alias " + key + "='" + session.getAlias( key ) +
                    "'" );
        }
    }
}
