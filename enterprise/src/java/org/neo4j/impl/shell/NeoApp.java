/*
 * Copyright (c) 2002-2008 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.impl.shell;

import java.rmi.RemoteException;

import org.neo4j.api.core.Direction;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.Transaction;
import org.neo4j.util.shell.AbstractApp;
import org.neo4j.util.shell.App;
import org.neo4j.util.shell.AppCommandParser;
import org.neo4j.util.shell.Output;
import org.neo4j.util.shell.Session;
import org.neo4j.util.shell.ShellException;

/**
 * An implementation of {@link App} which has common methods and functionality
 * to use with neo.
 */
public abstract class NeoApp extends AbstractApp
{
    private static final String NODE_KEY = "CURRENT_NODE";

    protected static Node getCurrentNode( NeoShellServer server, Session session )
    {
        Number id = (Number) safeGet( session, NODE_KEY );
        Node node = null;
        if ( id == null )
        {
            node = server.getNeo().getReferenceNode();
            setCurrentNode( session, node );
        }
        else
        {
            node = server.getNeo().getNodeById( id.longValue() );
        }
        return node;
    }

    protected Node getCurrentNode( Session session )
    {
        return getCurrentNode( getNeoServer(), session );
    }

    protected static void setCurrentNode( Session session, Node node )
    {
        safeSet( session, NODE_KEY, node.getId() );
    }

    protected NeoShellServer getNeoServer()
    {
        return (NeoShellServer) this.getServer();
    }

    protected RelationshipType getRelationshipType( String name )
    {
        return new NeoAppRelationshipType( name );
    }

    protected Direction getDirection( String direction ) throws ShellException
    {
        return this.getDirection( direction, Direction.OUTGOING );
    }

    protected Direction getDirection( String direction,
        Direction defaultDirection ) throws ShellException
    {
        if ( direction == null )
        {
            return defaultDirection;
        }

        Direction result = null;
        try
        {
            result = Direction.valueOf( direction.toUpperCase() );
        }
        catch ( Exception e )
        {
            if ( direction.equalsIgnoreCase( "o" ) )
            {
                result = Direction.OUTGOING;
            }
            else if ( direction.equalsIgnoreCase( "i" ) )
            {
                result = Direction.INCOMING;
            }
        }

        if ( result == null )
        {
            throw new ShellException( "Unknown direction " + direction
                + " (may be " + directionAlternatives() + ")" );
        }
        return result;
    }

    protected Node getNodeById( long id )
    {
        return this.getNeoServer().getNeo().getNodeById( id );
    }

    public final String execute( AppCommandParser parser, Session session,
        Output out ) throws ShellException
    {
        Transaction tx = getNeoServer().getNeo().beginTx();
        try
        {
            String result = this.exec( parser, session, out );
            tx.success();
            return result;
        }
        catch ( RemoteException e )
        {
            throw new ShellException( e );
        }
        finally
        {
            tx.finish();
        }
    }

    protected String directionAlternatives()
    {
        return "OUTGOING, INCOMING, o, i";
    }

    protected abstract String exec( AppCommandParser parser, Session session,
        Output out ) throws ShellException, RemoteException;

    protected String getDisplayNameForCurrentNode()
    {
        return "(me)";
    }

    /**
     * Returns the display name for a {@link Node}.
     * @param node
     *            the node to get the name-representation for.
     * @return the display name for a {@link Node}.
     */
    public static String getDisplayNameForNode( Node node )
    {
        return node != null ? getDisplayNameForNode( node.getId() )
            : getDisplayNameForNode( (Long) null );
    }

    /**
     * Returns the display name for a {@link Node}.
     * @param nodeId
     *            the node id to get the name-representation for.
     * @return the display name for a {@link Node}.
     */
    public static String getDisplayNameForNode( Long nodeId )
    {
        return "(" + nodeId + ")";
    }

    private static class NeoAppRelationshipType implements RelationshipType
    {
        private String name;

        private NeoAppRelationshipType( String name )
        {
            this.name = name;
        }

        public String name()
        {
            return this.name;
        }

        @Override
        public String toString()
        {
            return name();
        }
    }
}