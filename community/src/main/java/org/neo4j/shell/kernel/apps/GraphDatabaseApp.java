/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.shell.kernel.apps;

import java.lang.reflect.Array;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Expander;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.Traversal;
import org.neo4j.shell.App;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.OptionDefinition;
import org.neo4j.shell.OptionValueType;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.TextUtil;
import org.neo4j.shell.impl.AbstractApp;
import org.neo4j.shell.impl.AbstractClient;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;

/**
 * An implementation of {@link App} which has common methods and functionality
 * to use with neo4j.
 */
public abstract class GraphDatabaseApp extends AbstractApp
{
    private static final String CURRENT_KEY = "CURRENT_DIR";
    protected static final OptionDefinition OPTION_DEF_FOR_C = new OptionDefinition(
            OptionValueType.MUST,
            "Command to run for each returned node. Use $n for node-id, example:\n" +
            "-c \"ls -f name $n\". Multiple commands can be supplied with && in between" );

    /**
     * @param server the {@link GraphDatabaseShellServer} to get the current
     * node/relationship from.
     * @param session the {@link Session} used by the client.
     * @return the current node/relationship the client stands on
     * at the moment.
     * @throws ShellException if some error occured.
     */
    public static NodeOrRelationship getCurrent(
        GraphDatabaseShellServer server, Session session ) throws ShellException
    {
        String currentThing = ( String ) safeGet( session, CURRENT_KEY );
        NodeOrRelationship result = null;
        if ( currentThing == null )
        {
            result = NodeOrRelationship.wrap(
                server.getDb().getReferenceNode() );
            setCurrent( session, result );
        }
        else
        {
            TypedId typedId = new TypedId( currentThing );
            result = getThingById( server, typedId );
        }
        return result;
    }

    protected NodeOrRelationship getCurrent( Session session )
        throws ShellException
    {
        return getCurrent( getServer(), session );
    }

    public static boolean isCurrent( Session session, NodeOrRelationship thing )
    {
        String currentThing = ( String ) safeGet( session, CURRENT_KEY );
        return currentThing != null && currentThing.equals(
                thing.getTypedId().toString() );
    }

    protected static void setCurrent( Session session,
        NodeOrRelationship current )
    {
        safeSet( session, CURRENT_KEY, current.getTypedId().toString() );
    }

    protected void assertCurrentIsNode( Session session )
        throws ShellException
    {
        NodeOrRelationship current = getCurrent( session );
        if ( !current.isNode() )
        {
            throw new ShellException(
                "You must stand on a node to be able to do this" );
        }
    }

    @Override
    public GraphDatabaseShellServer getServer()
    {
        return ( GraphDatabaseShellServer ) super.getServer();
    }

    protected static RelationshipType getRelationshipType( String name )
    {
        return DynamicRelationshipType.withName( name );
    }

    protected static Direction getDirection( String direction ) throws ShellException
    {
        return getDirection( direction, Direction.OUTGOING );
    }

    protected static Direction getDirection( String direction,
        Direction defaultDirection ) throws ShellException
    {
        return ( Direction ) parseEnum( Direction.class, direction, defaultDirection );
    }

    protected static NodeOrRelationship getThingById(
        GraphDatabaseShellServer server, TypedId typedId ) throws ShellException
    {
        NodeOrRelationship result = null;
        if ( typedId.isNode() )
        {
            try
            {
                result = NodeOrRelationship.wrap(
                    server.getDb().getNodeById( typedId.getId() ) );
            }
            catch ( NotFoundException e )
            {
                throw new ShellException( "Node " + typedId.getId() +
                    " not found" );
            }
        }
        else
        {
            try
            {
                result = NodeOrRelationship.wrap(
                    server.getDb().getRelationshipById( typedId.getId() ) );
            }
            catch ( NotFoundException e )
            {
                throw new ShellException( "Relationship " + typedId.getId() +
                    " not found" );
            }
        }
        return result;
    }

    protected NodeOrRelationship getThingById( TypedId typedId )
        throws ShellException
    {
        return getThingById( getServer(), typedId );
    }

    protected Node getNodeById( long id )
    {
        return this.getServer().getDb().getNodeById( id );
    }
    
    public String execute( AppCommandParser parser, Session session,
        Output out ) throws Exception
    {
        Transaction tx = getServer().getDb().beginTx();
        try
        {
            String result = this.exec( parser, session, out );
            tx.success();
            return result;
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
        Output out ) throws Exception;

    private static String getDisplayNameForCurrent(
            GraphDatabaseShellServer server, Session session )
            throws ShellException
    {
        NodeOrRelationship current = getCurrent( server, session );
        return current.isNode() ? "(me)" : "<me>";
    }

    /**
     * @param server the {@link GraphDatabaseShellServer} to run at.
     * @param session the {@link Session} used by the client.
     * @param thing the thing to get the name-representation for.
     * @return the display name for a {@link Node}.
     */
    public static String getDisplayName( GraphDatabaseShellServer server,
        Session session, NodeOrRelationship thing, boolean checkForMe )
        throws ShellException
    {
        if ( thing.isNode() )
        {
            return getDisplayName( server, session, thing.asNode(),
                    checkForMe );
        }
        else
        {
            return getDisplayName( server, session, thing.asRelationship(),
                true, checkForMe );
        }
    }

    /**
     * @param server the {@link GraphDatabaseShellServer} to run at.
     * @param session the {@link Session} used by the client.
     * @param typedId the id for the item to display.
     * @return a display string for the {@code typedId}.
     * @throws ShellException if an error occurs.
     */
    public static String getDisplayName( GraphDatabaseShellServer server,
        Session session, TypedId typedId, boolean checkForMe )
        throws ShellException
    {
        return getDisplayName( server, session,
            getThingById( server, typedId ), checkForMe );
    }

    /**
     * @param server the {@link GraphDatabaseShellServer} to run at.
     * @param session the {@link Session} used by the client.
     * @param node the {@link Node} to get a display string for.
     * @return a display string for {@code node}.
     */
    public static String getDisplayName( GraphDatabaseShellServer server,
        Session session, Node node, boolean checkForMe ) throws ShellException
    {
        if ( checkForMe &&
                isCurrent( session, NodeOrRelationship.wrap( node ) ) )
        {
            return getDisplayNameForCurrent( server, session );
        }

        String title = findTitle( server, session, node );
        StringBuilder result = new StringBuilder( "(" );
        result.append( (title != null ? title + "," : "" ) );
        result.append( node.getId() );
        result.append( ")" );
        return result.toString();
    }

    protected static String findTitle( GraphDatabaseShellServer server,
        Session session, Node node )
    {
        String keys = ( String ) safeGet( session,
            AbstractClient.TITLE_KEYS_KEY );
        if ( keys == null )
        {
            return null;
        }

        String[] titleKeys = keys.split( Pattern.quote( "," ) );
        Pattern[] patterns = new Pattern[ titleKeys.length ];
        for ( int i = 0; i < titleKeys.length; i++ )
        {
            patterns[ i ] = Pattern.compile( titleKeys[ i ] );
        }
        for ( Pattern pattern : patterns )
        {
            for ( String nodeKey : node.getPropertyKeys() )
            {
                if ( matches( pattern, nodeKey, false, false ) )
                {
                    return trimLength( session,
                        format( node.getProperty( nodeKey ), false ) );
                }
            }
        }
        return null;
    }

    private static String trimLength( Session session, String string )
    {
        String maxLengthString = ( String )
            safeGet( session, AbstractClient.TITLE_MAX_LENGTH );
        int maxLength = maxLengthString != null ?
            Integer.parseInt( maxLengthString ) : Integer.MAX_VALUE;
        if ( string.length() > maxLength )
        {
            string = string.substring( 0, maxLength ) + "...";
        }
        return string;
    }

    /**
     * @param server the {@link GraphDatabaseShellServer} to run at.
     * @param session the {@link Session} used by the client.
     * @param relationship the {@link Relationship} to get a display name for.
     * @param verbose whether or not to include the relationship id as well.
     * @return a display string for the {@code relationship}.
     */
    public static String getDisplayName( GraphDatabaseShellServer server,
        Session session, Relationship relationship, boolean verbose,
        boolean checkForMe ) throws ShellException
    {
        if ( checkForMe &&
                isCurrent( session, NodeOrRelationship.wrap( relationship ) ) )
        {
            return getDisplayNameForCurrent( server, session );
        }

        StringBuilder result = new StringBuilder( "[" );
        result.append( relationship.getType().name() );
        result.append( verbose ? "," + relationship.getId() : "" );
        result.append( "]" );
        return result.toString();
    }
    
    public static String withArrows( Relationship relationship, String displayName, Node leftNode )
    {
        if ( relationship.getStartNode().equals( leftNode ) )
        {
            return " --" + displayName + "-> ";
        }
        else if ( relationship.getEndNode().equals( leftNode ) )
        {
            return " <-" + displayName + "-- ";
        }
        throw new IllegalArgumentException( leftNode + " is neither start nor end node to " + relationship );
    }

    protected static String fixCaseSensitivity( String string,
        boolean caseInsensitive )
    {
        return caseInsensitive ? string.toLowerCase() : string;
    }

    protected static Pattern newPattern( String pattern,
        boolean caseInsensitive )
    {
        return pattern == null ? null : Pattern.compile(
            fixCaseSensitivity( pattern, caseInsensitive ) );
    }

    protected static boolean matches( Pattern patternOrNull, String value,
        boolean caseInsensitive, boolean loose )
    {
        if ( patternOrNull == null )
        {
            return true;
        }

        value = fixCaseSensitivity( value, caseInsensitive );
        return loose ?
            patternOrNull.matcher( value ).find() :
            patternOrNull.matcher( value ).matches();
    }

    protected static <T extends Enum<T>> Enum<T> parseEnum(
        Class<T> enumClass, String name, Enum<T> defaultValue  )
    {
        if ( name == null )
        {
            return defaultValue;
        }

        name = name.toLowerCase();
        for ( T enumConstant : enumClass.getEnumConstants() )
        {
            if ( enumConstant.name().equalsIgnoreCase( name ) )
            {
                return enumConstant;
            }
        }
        for ( T enumConstant : enumClass.getEnumConstants() )
        {
            if ( enumConstant.name().toLowerCase().startsWith( name ) )
            {
                return enumConstant;
            }
        }
        throw new IllegalArgumentException( "No '" + name + "' or '" +
            name + ".*' in " + enumClass );
    }

    protected static String frame( String string, boolean frame )
    {
        return frame ? "[" + string + "]" : string;
    }

    protected static String format( Object value, boolean includeFraming )
    {
        String result = null;
        if ( value.getClass().isArray() )
        {
            StringBuilder buffer = new StringBuilder();
            int length = Array.getLength( value );
            for ( int i = 0; i < length; i++ )
            {
                Object singleValue = Array.get( value, i );
                if ( i > 0 )
                {
                    buffer.append( "," );
                }
                buffer.append( frame( singleValue.toString(),
                    includeFraming ) );
            }
            result = buffer.toString();
        }
        else
        {
            result = frame( value.toString(), includeFraming );
        }
        return result;
    }

    protected static void printAndInterpretTemplateLines( Collection<String> templateLines,
            boolean forcePrintHitHeader, boolean newLineBetweenHits, Node node,
            GraphDatabaseShellServer server, Session session, Output out )
            throws ShellException, RemoteException
    {
        if ( templateLines.isEmpty() || forcePrintHitHeader )
        {
            out.println( getDisplayName( server, session, node, true ) );
        }

        if ( !templateLines.isEmpty() )
        {
            Map<String, Object> data = new HashMap<String, Object>();
            data.put( "n", node.getId() );
            for ( String command : templateLines )
            {
                String line = TextUtil.templateString( command, data );
                server.interpretLine( line, session, out );
            }
        }
        if ( newLineBetweenHits )
        {
            out.println();
        }
    }

    protected static RelationshipExpander toExpander( GraphDatabaseService db, Direction defaultDirection,
            Map<String, Object> filterMap, boolean caseInsensitiveFilters, boolean looseFilters ) throws ShellException
    {
        defaultDirection = defaultDirection != null ? defaultDirection : Direction.BOTH;
        Expander expander = Traversal.emptyExpander();
        boolean addedSomething = false;
        for ( RelationshipType type : db.getRelationshipTypes() )
        {
            Direction direction = null;
            if ( filterMap == null || filterMap.isEmpty() )
            {
                direction = defaultDirection;
            }
            else
            {
                for ( Map.Entry<String, Object> entry : filterMap.entrySet() )
                {
                    if ( matches( newPattern( entry.getKey(), caseInsensitiveFilters ),
                        type.name(), caseInsensitiveFilters, looseFilters ) )
                    {
                        direction = getDirection( entry.getValue() != null ? entry.getValue().toString() : null, defaultDirection );
                        break;
                    }
                }
            }
    
            // It matches
            if ( direction != null )
            {
                expander = expander.add( type, direction );
                addedSomething = true;
            }
        }
        
        if ( !filterMap.isEmpty() && !addedSomething )
        {
            return null;
        }
        return expander;
    }
}