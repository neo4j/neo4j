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

import static java.lang.Integer.parseInt;
import static org.neo4j.shell.kernel.apps.NodeOrRelationship.wrap;

import java.rmi.RemoteException;

import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.Service;
import org.neo4j.shell.App;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Continuation;
import org.neo4j.shell.OptionDefinition;
import org.neo4j.shell.OptionValueType;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;

@Service.Implementation( App.class )
public class Rmnode extends TransactionProvidingApp
{
    public Rmnode()
    {
        addOptionDefinition( "f", new OptionDefinition( OptionValueType.NONE,
                "Force deletion, will delete all relationships prior to deleting the node" ) );
    }
    
    @Override
    public String getDescription()
    {
        return "Deletes a node from the graph. If no node-id argument is given the current node is deleted";
    }

    @Override
    protected Continuation exec( AppCommandParser parser, Session session, Output out )
            throws ShellException, RemoteException
    {
        NodeOrRelationship node = null;
        if ( parser.arguments().isEmpty() )
        {
            node = getCurrent( session );
        }
        else
        {
            long id = parseInt( parser.arguments().get( 0 ) );
            try
            {
                node = wrap( getNodeById( id ) );
            }
            catch ( NotFoundException e )
            {
                throw new ShellException( "No node " + id + " found" );
            }
        }
        
        if ( !node.isNode() )
        {
            out.println( "Please select a node to delete" );
            return Continuation.INPUT_COMPLETE;
        }
        
        boolean forceDeletion = parser.options().containsKey( "f" );
        if ( forceDeletion )
        {
            for ( Relationship relationship : node.asNode().getRelationships() )
            {
                out.println( "Relationship " + getDisplayName( getServer(), session, relationship, true, false ) + " deleted" );
                relationship.delete();
            }
        }
        
        if ( node.asNode().hasRelationship() )
        {
            throw new ShellException( getDisplayName( getServer(), session, node.asNode(), false ) +
                    " cannot be deleted because it still has relationships. Use -f to force deletion of its relationships" );
        }
        node.asNode().delete();
        return Continuation.INPUT_COMPLETE;
    }
}
