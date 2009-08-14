/*
 * Copyright (c) 2002-2009 "Neo Technology,"
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
package org.neo4j.impl.shell.apps;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.impl.shell.NeoApp;
import org.neo4j.impl.shell.apps.NodeOrRelationship.TypedId;
import org.neo4j.util.shell.AppCommandParser;
import org.neo4j.util.shell.OptionValueType;
import org.neo4j.util.shell.Output;
import org.neo4j.util.shell.Session;
import org.neo4j.util.shell.ShellException;

/**
 * Mimics the POSIX application with the same name, i.e. traverses to a node.
 */
public class Cd extends NeoApp
{
    /**
     * The {@link Session} key to use to store the current node and working
     * directory (i.e. the path which the client got to it).
     */
    public static final String WORKING_DIR_KEY = "WORKING_DIR";

    /**
     * Constructs a new cd application.
     */
    public Cd()
    {
        this.addValueType( "a", new OptionContext( OptionValueType.NONE,
            "Absolute id, new primitive doesn't need to be connected to " +
            "the\ncurrent one" ) );
        this.addValueType( "r", new OptionContext( OptionValueType.NONE,
            "Makes the supplied id represent a relationship instead of " +
            "a node" ) );
    }

    @Override
    public String getDescription()
    {
        return "Changes the current node or relationship, i.e. traverses " +
       		"one step to another\nnode or relationship. Usage: cd <id>";
    }

    @Override
    protected String exec( AppCommandParser parser, Session session,
        Output out ) throws ShellException, RemoteException
    {
        List<TypedId> paths = readPaths( session );

        NodeOrRelationship current = getCurrent( session );
        NodeOrRelationship newThing = null;
        if ( parser.arguments().isEmpty() )
        {
            newThing = NodeOrRelationship.wrap(
                getNeoServer().getNeo().getReferenceNode() );
            paths.clear();
        }
        else
        {
            String arg = parser.arguments().get( 0 );
            TypedId newId = current.getTypedId();
            if ( arg.equals( ".." ) )
            {
                if ( paths.size() > 0 )
                {
                    newId = paths.remove( paths.size() - 1 );
                }
            }
            else if ( arg.equals( "." ) )
            {
            }
            else if ( arg.equals( "start" ) || arg.equals( "end" ) )
            {
                newId = getStartOrEnd( current, arg );
                paths.add( current.getTypedId() );
            }
            else
            {
                long suppliedId = Long.parseLong( arg );
                if ( parser.options().containsKey( "r" ) )
                {
                    newId = new TypedId( NodeOrRelationship.TYPE_RELATIONSHIP,
                        suppliedId );
                }
                else
                {
                    newId = new TypedId( NodeOrRelationship.TYPE_NODE,
                        suppliedId );
                }
                if ( newId.equals( current.getTypedId() ) )
                {
                    throw new ShellException( "Can't cd to where you stand" );
                }
                boolean absolute = parser.options().containsKey( "a" );
                if ( !absolute && !this.isConnected( current, newId ) )
                {
                    throw new ShellException(
                        getDisplayName( getNeoServer(), session, newId ) +
                        " isn't connected to the current primitive," +
                        " use -a to force it to go there anyway" );
                }
                paths.add( current.getTypedId() );
            }
            newThing = this.getThingById( newId );
        }

        setCurrent( session, newThing );
        session.set( WORKING_DIR_KEY, this.makePath( paths ) );
        return null;
    }

    private TypedId getStartOrEnd( NodeOrRelationship current, String arg )
        throws ShellException
    {
        if ( !current.isRelationship() )
        {
            throw new ShellException( "Only allowed on relationships" );
        }
        Node newNode = null;
        if ( arg.equals( "start" ) )
        {
            newNode = current.asRelationship().getStartNode();
        }
        else if ( arg.equals( "end" ) )
        {
            newNode = current.asRelationship().getEndNode();
        }
        else
        {
            throw new ShellException( "Unknown alias '" + arg + "'" );
        }
        return NodeOrRelationship.wrap( newNode ).getTypedId();
    }

    private boolean isConnected( NodeOrRelationship current, TypedId newId )
        throws ShellException
    {
        if ( current.isNode() )
        {
            Node currentNode = current.asNode();
            for ( Relationship rel : currentNode.getRelationships() )
            {
                if ( newId.isNode() )
                {
                    if ( rel.getOtherNode( currentNode ).getId() ==
                        newId.getId() )
                    {
                        return true;
                    }
                }
                else
                {
                    if ( rel.getId() == newId.getId() )
                    {
                        return true;
                    }
                }
            }
        }
        else
        {
            if ( newId.isRelationship() )
            {
                return false;
            }
            
            Relationship relationship = current.asRelationship();
            if ( relationship.getStartNode().getId() == newId.getId() ||
                relationship.getEndNode().getId() == newId.getId() )
            {
                return true;
            }
        }
        return false;
    }

    /**
     * Reads the session variable specified in {@link #WORKING_DIR_KEY} and
     * returns it as a list of {@link TypedId}s.
     * @param session the session to read from.
     * @return the working directory as a list.
     * @throws RemoteException if an RMI error occurs.
     */
    public static List<TypedId> readPaths( Session session )
        throws RemoteException
    {
        List<TypedId> list = new ArrayList<TypedId>();
        String path = (String) session.get( WORKING_DIR_KEY );
        if ( path != null && path.trim().length() > 0 )
        {
            for ( String typedId : path.split( "," ) )
            {
                list.add( new TypedId( typedId ) );
            }
        }
        return list;
    }

    private String makePath( List<TypedId> paths )
    {
        StringBuffer buffer = new StringBuffer();
        for ( TypedId typedId : paths )
        {
            if ( buffer.length() > 0 )
            {
                buffer.append( "," );
            }
            buffer.append( typedId.toString() );
        }
        return buffer.length() > 0 ? buffer.toString() : null;
    }
}