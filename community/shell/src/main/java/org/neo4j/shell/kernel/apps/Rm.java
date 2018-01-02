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
package org.neo4j.shell.kernel.apps;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.helpers.Service;
import org.neo4j.shell.App;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Continuation;
import org.neo4j.shell.OptionDefinition;
import org.neo4j.shell.OptionValueType;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;

/**
 * Mimics the POSIX application with the same name, i.e. removes a property from
 * a node or relationship. It could also (regarding POSIX) delete nodes,
 * but it doesn't.
 */
@Service.Implementation( App.class )
public class Rm extends TransactionProvidingApp
{
    {
        addOptionDefinition( "p", new OptionDefinition( OptionValueType.NONE,
                "Removes a property" ) );
        addOptionDefinition( "l", new OptionDefinition( OptionValueType.MUST,
                "Removes one or more labels" ) );
    }
    
    @Override
    public String getDescription()
    {
        return "Removes a property from the current node or relationship or label from the current node.\n" +
            "Usage:\n" +
            "  rm <key>\n" +
            "  rm -p name\n" +
            "  rm -l PERSON";
    }

    @Override
    protected Continuation exec( AppCommandParser parser, Session session,
        Output out ) throws Exception
    {
        NodeOrRelationship thing = getCurrent( session );
        boolean forProperty = parser.options().containsKey( "p" );
        boolean forLabel = parser.options().containsKey( "l" );
        if ( forProperty || !forLabel )
        {   // Property
            if ( parser.arguments().isEmpty() )
            {
                throw new ShellException( "Must supply the property key or label name to " +
                    "remove, like: rm title" );
            }
            
            String key = parser.arguments().get( 0 );
            if ( thing.removeProperty( key ) == null )
            {
                out.println( "Property '" + key + "' not found" );
            }
        }
        else
        {
            Node node = thing.asNode();
            for ( Label label : parseLabels( parser ) )
            {
                node.removeLabel( label );
            }
        }
        return Continuation.INPUT_COMPLETE;
    }
}