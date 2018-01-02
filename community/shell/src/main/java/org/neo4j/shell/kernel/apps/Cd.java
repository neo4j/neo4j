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

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;

import org.neo4j.graphdb.Node;
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
import org.neo4j.shell.impl.RelationshipToNodeIterable;

import static org.neo4j.shell.TextUtil.lastWordOrQuoteOf;

/**
 * Mimics the POSIX application with the same name, i.e. traverses to a node.
 */
@Service.Implementation( App.class )
public class Cd extends TransactionProvidingApp
{
    private static final String START_ALIAS = "start";
    private static final String END_ALIAS = "end";

    /**
     * Constructs a new cd application.
     */
    public Cd()
    {
        this.addOptionDefinition( "a", new OptionDefinition( OptionValueType.NONE,
            "Absolute id, new primitive doesn't need to be connected to the current one" ) );
        this.addOptionDefinition( "r", new OptionDefinition( OptionValueType.NONE,
            "Makes the supplied id represent a relationship instead of a node" ) );
    }

    @Override
    public String getDescription()
    {
        return "Changes the current node or relationship, i.e. traverses " +
       		"one step to another node or relationship. Usage: cd <id>";
    }

    @Override
    protected List<String> completionCandidatesInTx( String partOfLine, Session session ) throws ShellException
    {
        String lastWord = lastWordOrQuoteOf( partOfLine, false );
        if ( lastWord.startsWith( "-" ) )
        {
            return super.completionCandidates( partOfLine, session );
        }

        NodeOrRelationship current;
        try
        {
            current = getCurrent( session );
        }
        catch ( ShellException e )
        {
            return Collections.emptyList();
        }
        
        TreeSet<String> result = new TreeSet<>();
        if ( current.isNode() )
        {
            // TODO Check if -r is supplied
            Node node = current.asNode();
            for ( Node otherNode : RelationshipToNodeIterable.wrap(
                    node.getRelationships(), node ) )
            {
                long otherNodeId = otherNode.getId();
                String title = findTitle( session, otherNode );
                if ( title != null )
                {
                    if ( !result.contains( title ) )
                    {
                        maybeAddCompletionCandidate( result, title + "," + otherNodeId,
                                lastWord );
                    }
                }
                maybeAddCompletionCandidate( result, "" + otherNodeId, lastWord );
            }
        }
        else
        {
            maybeAddCompletionCandidate( result, START_ALIAS, lastWord );
            maybeAddCompletionCandidate( result, END_ALIAS, lastWord );
            Relationship rel = current.asRelationship();
            maybeAddCompletionCandidate( result, "" + rel.getStartNode().getId(), lastWord );
            maybeAddCompletionCandidate( result, "" + rel.getEndNode().getId(), lastWord );
        }
        return new ArrayList<>( result );
    }

    private static void maybeAddCompletionCandidate( Collection<String> candidates,
            String candidate, String lastWord )
    {
        if ( lastWord.length() == 0 || candidate.startsWith( lastWord ) )
        {
            candidates.add( candidate );
        }
    }

    @Override
    protected Continuation exec( AppCommandParser parser, Session session,
        Output out ) throws ShellException, RemoteException
    {
        List<TypedId> paths = readCurrentWorkingDir( session );

        NodeOrRelationship newThing = null;
        if ( parser.arguments().isEmpty() )
        {
            clearCurrent( session );
            writeCurrentWorkingDir( paths, session );
            return Continuation.INPUT_COMPLETE;
        }
        else
        {
            NodeOrRelationship current = null;
            try
            {
                current = getCurrent( session );
            }
            catch ( ShellException e )
            { // Ok, didn't exist
            }
            
            String arg = parser.arguments().get( 0 );
            TypedId newId = null;
            if ( arg.equals( ".." ) )
            {
                if ( paths.size() > 0 )
                {
                    newId = paths.remove( paths.size() - 1 );
                }
            }
            else if ( arg.equals( "." ) )
            {   // Do nothing
            }
            else if ( arg.equals( START_ALIAS ) || arg.equals( END_ALIAS ) )
            {
                if ( current == null )
                {
                    throw new ShellException( "Can't do " + START_ALIAS + " or " +
                            END_ALIAS + " on a non-existent relationship" );
                }
                
                newId = getStartOrEnd( current, arg );
                paths.add( current.getTypedId() );
            }
            else
            {
                long suppliedId = -1;
                try
                {
                    suppliedId = Long.parseLong( arg );
                }
                catch ( NumberFormatException e )
                {
                    if ( current != null )
                    {
                        suppliedId = findNodeWithTitle( current.asNode(), arg, session );
                    }
                    if ( suppliedId == -1 )
                    {
                        throw new ShellException( "No connected node with title '" + arg + "'" );
                    }
                }

                newId = parser.options().containsKey( "r" ) ?
                    new TypedId( NodeOrRelationship.TYPE_RELATIONSHIP, suppliedId ) :
                    new TypedId( NodeOrRelationship.TYPE_NODE, suppliedId );
                if ( current != null && newId.equals( current.getTypedId() ) )
                {
                    throw new ShellException( "Can't cd to where you stand" );
                }
                boolean absolute = parser.options().containsKey( "a" );
                if ( !absolute && current != null && !isConnected( current, newId ) )
                {
                    throw new ShellException(
                        getDisplayName( getServer(), session, newId, false ) +
                        " isn't connected to the current primitive," +
                        " use -a to force it to go there anyway" );
                }
                
                if ( current != null )
                {
                    paths.add( current.getTypedId() );
                }
            }
            newThing = newId != null ? getThingById( newId ) : current;
        }

        if ( newThing != null )
        {
            setCurrent( session, newThing );
        }
        else
        {
            clearCurrent( session );
        }
        writeCurrentWorkingDir( paths, session );
        return Continuation.INPUT_COMPLETE;
    }

    private long findNodeWithTitle( Node node, String match, Session session ) throws ShellException
    {
        Object[] matchParts = splitNodeTitleAndId( match );
        if ( matchParts[1] != null )
        {
            return (Long) matchParts[1];
        }

        String titleMatch = (String) matchParts[0];
        for ( Node otherNode : RelationshipToNodeIterable.wrap( node.getRelationships(), node ) )
        {
            String title = findTitle( session, otherNode );
            if ( titleMatch.equals( title ) )
            {
                return otherNode.getId();
            }
        }
        return -1;
    }

    private Object[] splitNodeTitleAndId( String string )
    {
        int index = string.lastIndexOf( "," );
        String title = null;
        Long id = null;
        try
        {
            id = Long.parseLong( string.substring( index + 1 ) );
            title = string.substring( 0, index );
        }
        catch ( NumberFormatException e )
        {
            title = string;
        }
        return new Object[] { title, id };
    }

    private TypedId getStartOrEnd( NodeOrRelationship current, String arg )
        throws ShellException
    {
        if ( !current.isRelationship() )
        {
            throw new ShellException( "Only allowed on relationships" );
        }
        Node newNode = null;
        if ( arg.equals( START_ALIAS ) )
        {
            newNode = current.asRelationship().getStartNode();
        }
        else if ( arg.equals( END_ALIAS ) )
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
    {
        if ( current.isNode() )
        {
            Node currentNode = current.asNode();
            long startTime = System.currentTimeMillis();
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
                if ( System.currentTimeMillis()-startTime > 350 )
                {
                    // DOn't spend too long time in here
                    return true;
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
}