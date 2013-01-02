/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.neo4j.graphdb.Node;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.shell.App;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Continuation;
import org.neo4j.shell.OptionDefinition;
import org.neo4j.shell.OptionValueType;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;

@Service.Implementation( App.class )
public class Mknode extends GraphDatabaseApp
{
    {
        addOptionDefinition( "np", new OptionDefinition( OptionValueType.MUST,
                "Properties (a json map) to set for the new node (if one is created)" ) );
        addOptionDefinition( "cd", new OptionDefinition( OptionValueType.NONE,
                "Go to the created node, like doing 'cd'" ) );
        addOptionDefinition( "v", new OptionDefinition( OptionValueType.NONE,
                "Verbose mode: display created node" ) );
//        addOptionDefinition( "r", new OptionDefinition( OptionValueType.NONE,
//                "Sets this new node as the referende node for this database (if there is no reference node already set)" ) );
    }
    
    @Override
    public String getDescription()
    {
        return "Creates a new node, f.ex:\n" +
        		"mknode --cd --np \"{'name':'Neo'}\"";
    }
    
    @Override
    protected Continuation exec( AppCommandParser parser, Session session, Output out ) throws Exception
    {
        GraphDatabaseAPI db = getServer().getDb();
        Node node = null;
//        if ( parser.options().containsKey( "r" ) )
//        {
//            try
//            {
//                db.getReferenceNode();
//                throw new ShellException( "Reference node already exists" );
//            }
//            catch ( NotFoundException e )
//            {
//                node = ((AbstractGraphDatabase)getServer().getDb()).getConfig().getGraphDbModule().createNewReferenceNode();
//            }
//        }
//        else
//        {
            node = db.createNode();
//        }
        
        setProperties( node, parser.option( "np", null ) );
        if ( parser.options().containsKey( "cd" ) ) cdTo( session, node );
        if ( parser.options().containsKey( "v" ) )
        {
            out.println( "Node " + getDisplayName( getServer(), session, node, false ) + " created" );
        }
        return Continuation.INPUT_COMPLETE;
    }
}
