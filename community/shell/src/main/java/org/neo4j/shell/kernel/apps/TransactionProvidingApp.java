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

import java.lang.reflect.Array;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.neo4j.function.Function;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Expander;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.OrderedByTypeExpander;
import org.neo4j.kernel.Traversal;
import org.neo4j.shell.App;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Continuation;
import org.neo4j.shell.OptionDefinition;
import org.neo4j.shell.OptionValueType;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.TextUtil;
import org.neo4j.shell.impl.AbstractApp;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;
import org.neo4j.shell.util.json.JSONArray;
import org.neo4j.shell.util.json.JSONException;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.neo4j.shell.ShellException.stackTraceAsString;

/**
 * An implementation of {@link App} which has common methods and functionality
 * to use with neo4j.
 */
public abstract class TransactionProvidingApp extends AbstractApp
{
    private static final Label[] EMPTY_LABELS = new Label[0];
    private static final RelationshipType[] EMPTY_REL_TYPES = new RelationshipType[0];

    private static final Function<String[],Label[]> CREATE_LABELS = new Function<String[],Label[]>()
    {
        @Override
        public Label[] apply( String[] values )
        {
            Label[] labels = new Label[values.length];
            for ( int i = 0; i < values.length; i++ )
            {
                labels[i] = DynamicLabel.label( values[i] );
            }
            return labels;
        }
    };

    private static final Function<String[],RelationshipType[]> CREATE_REL_TYPES = new Function<String[],RelationshipType[]>()
    {
        @Override
        public RelationshipType[] apply( String[] values )
        {
            RelationshipType[] types = new RelationshipType[values.length];
            for ( int i = 0; i < values.length; i++ )
            {
                types[i] = DynamicRelationshipType.withName( values[i] );
            }
            return types;
        }
    };

    protected static final String[] STANDARD_EVAL_IMPORTS = new String[] {
        "org.neo4j.graphdb",
        "org.neo4j.graphdb.event",
        "org.neo4j.graphdb.index",
        "org.neo4j.graphdb.traversal",
        "org.neo4j.kernel"
    };

    protected static final OptionDefinition OPTION_DEF_FOR_C = new OptionDefinition(
            OptionValueType.MUST,
            "Command to run for each returned node. Use $i for node/relationship id, example:\n" +
            "-c \"ls -f name $i\". Multiple commands can be supplied with && in between" );

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
        String currentThing = session.getCurrent();
        NodeOrRelationship result;
        /*                           Note: Artifact of removing the ref node, revisit and clean up */
        if ( currentThing == null || currentThing.equals( "(?)" )  )
        {
            throw new ShellException( "Not currently standing on any entity." );
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

    public static boolean isCurrent( Session session, NodeOrRelationship thing ) throws ShellException
    {
        String currentThing = session.getCurrent();
        return currentThing != null && currentThing.equals(
                thing.getTypedId().toString() );
    }

    protected static void clearCurrent( Session session )
    {
        session.setCurrent( getDisplayNameForNonExistent());
    }

    protected static void setCurrent( Session session,
                                      NodeOrRelationship current ) throws ShellException
    {
        session.setCurrent( current.getTypedId().toString() );
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
        return parseEnum( Direction.class, direction, defaultDirection );
    }

    protected static NodeOrRelationship getThingById(
        GraphDatabaseShellServer server, TypedId typedId ) throws ShellException
    {
        NodeOrRelationship result;
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

    @Override
    public Continuation execute( AppCommandParser parser, Session session,
        Output out ) throws Exception
    {
        try (Transaction tx = getServer().getDb().beginTx())
        {
            getServer().registerTopLevelTransactionInProgress( session.getId() );
            Continuation result = this.exec( parser, session, out );
            if ( result == Continuation.EXCEPTION_CAUGHT )
            {
                tx.failure();
            }
            else
            {
                tx.success();
            }
            return result;
        }
    }

    @Override
    public final List<String> completionCandidates( String partOfLine, Session session ) throws ShellException
    {
        try ( Transaction tx = getServer().getDb().beginTx() )
        {
            List<String> result = completionCandidatesInTx( partOfLine, session );
            tx.success();
            return result;
        }
    }

    protected List<String> completionCandidatesInTx( String partOfLine, Session session ) throws ShellException
    {
        /*
         * Calls super of the non-tx version (completionCandidates). In an implementation the call hierarchy would be:
         *
         * TransactionProvidingApp.completionCandidates()
         *    --> MyApp.completionCandidatesInTx() - calls super.completionCandidatesInTx()
         *       --> TransactionProvidingApp.completionCandidatesInTx()
         *          --> AbstractApp.completionCandidates()
         */
        return super.completionCandidates( partOfLine, session );
    }

    protected String directionAlternatives()
    {
        return "OUTGOING, INCOMING, o, i";
    }

    protected abstract Continuation exec( AppCommandParser parser, Session session,
        Output out ) throws Exception;

    protected void printPath( Path path, boolean quietPrint, Session session, Output out )
            throws RemoteException, ShellException
    {
        StringBuilder builder = new StringBuilder();
        Node currentNode = null;
        for ( PropertyContainer entity : path )
        {
            String display;
            if ( entity instanceof Relationship )
            {
                display = quietPrint ? "" : getDisplayName( getServer(), session, (Relationship) entity, false, true );
                display = withArrows( (Relationship) entity, display, currentNode );
            }
            else
            {
                currentNode = (Node) entity;
                display = getDisplayName( getServer(), session, currentNode, true );
            }
            builder.append( display );
        }
        out.println( builder.toString() );
    }

    protected void setProperties( PropertyContainer entity, String propertyJson ) throws ShellException
    {
        if ( propertyJson == null )
        {
            return;
        }

        try
        {
            Map<String, Object> properties = parseJSONMap( propertyJson );
            for ( Map.Entry<String, Object> entry : properties.entrySet() )
            {
                entity.setProperty( entry.getKey(), jsonToNeo4jPropertyValue( entry.getValue() ) );
            }
        }
        catch ( JSONException e )
        {
            throw ShellException.wrapCause( e );
        }
    }

    private Object jsonToNeo4jPropertyValue( Object value ) throws ShellException
    {
        try
        {
            if ( value instanceof JSONArray )
            {
                JSONArray array = (JSONArray) value;
                Object firstItem = array.get( 0 );
                Object resultArray = Array.newInstance( firstItem.getClass(), array.length() );
                for ( int i = 0; i < array.length(); i++ )
                {
                    Array.set( resultArray, i, array.get( i ) );
                }
                return resultArray;
            }
            return value;
        }
        catch ( JSONException e )
        {
            throw new ShellException( stackTraceAsString( e ) );
        }
    }

    protected void cdTo( Session session, Node node ) throws RemoteException, ShellException
    {
        List<TypedId> wd = readCurrentWorkingDir( session );
        try
        {
            wd.add( getCurrent( session ).getTypedId() );
        }
        catch ( ShellException e )
        {   // OK not found then
        }
        writeCurrentWorkingDir( wd, session );
        setCurrent( session, NodeOrRelationship.wrap( node ) );
    }

    private static String getDisplayNameForCurrent(
            GraphDatabaseShellServer server, Session session )
            throws ShellException
    {
        NodeOrRelationship current = getCurrent( server, session );
        return current.isNode() ? "(me)" : "<me>";
    }

    public static String getDisplayNameForNonExistent()
    {
        return "(?)";
    }

    /**
     * @param server the {@link GraphDatabaseShellServer} to run at.
     * @param session the {@link Session} used by the client.
     * @param thing the thing to get the name-representation for.
     * @param checkForMe check if node/rel is the current one in the session
     * @return the display name for a {@link Node}.
     * @throws ShellException if an error occurs.
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
     * @param checkForMe check if node/rel is the current one in the session
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
     * @param checkForMe check if node is the current one in the session
     * @return a display string for {@code node}.
     * @throws ShellException if an error occurs.
     */
    public static String getDisplayName( GraphDatabaseShellServer server,
        Session session, Node node, boolean checkForMe ) throws ShellException
    {
        if ( checkForMe &&
                isCurrent( session, NodeOrRelationship.wrap( node ) ) )
        {
            return getDisplayNameForCurrent( server, session );
        }

        String title = findTitle( session, node );
        StringBuilder result = new StringBuilder( "(" );
        result.append( title != null ? title + "," : "" );
        result.append( node.getId() );
        result.append( ")" );
        return result.toString();
    }

    protected static String findTitle( Session session, Node node ) throws ShellException
    {
        String keys = session.getTitleKeys();
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

    private static String trimLength( Session session, String string ) throws ShellException
    {
        String maxLengthString = session.getMaxTitleLength();
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
     * @param checkForMe check if relationship is the current one in the session
     * @return a display string for the {@code relationship}.
     * @throws ShellException if an error occurs.
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
        result.append( ":" ).append( relationship.getType().name() );
        result.append( verbose ? "," + relationship.getId() : "" );
        result.append( "]" );
        return result.toString();
    }

    public static String withArrows( Relationship relationship, String displayName, Node leftNode )
    {
        if ( relationship.getStartNode().equals( leftNode ) )
        {
            return "-" + displayName + "->";
        }
        else if ( relationship.getEndNode().equals( leftNode ) )
        {
            return "<-" + displayName + "-";
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

    protected static <T extends Enum<T>> String niceEnumAlternatives( Class<T> enumClass )
    {
        StringBuilder builder = new StringBuilder( "[" );
        int count = 0;
        for ( T enumConstant : enumClass.getEnumConstants() )
        {
            builder.append( count++ == 0 ? "" : ", " );
            builder.append( enumConstant.name() );
        }
        return builder.append( "]" ).toString();
    }

    protected static <T extends Enum<T>> T parseEnum(
        Class<T> enumClass, String name, T defaultValue, Pair<String, T>... additionalPairs )
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

        for ( Pair<String, T> additional : additionalPairs )
        {
            if ( additional.first().equalsIgnoreCase( name ) )
            {
                return additional.other();
            }
        }
        for ( Pair<String, T> additional : additionalPairs )
        {
            if ( additional.first().toLowerCase().startsWith( name ) )
            {
                return additional.other();
            }
        }

        throw new IllegalArgumentException( "No '" + name + "' or '" +
            name + ".*' in " + enumClass );
    }

    protected static boolean filterMatches( Map<String, Object> filterMap, boolean caseInsensitiveFilters,
            boolean looseFilters, String key, Object value )
    {
        if ( filterMap == null || filterMap.isEmpty() )
        {
            return true;
        }
        for ( Map.Entry<String, Object> filter : filterMap.entrySet() )
        {
            if ( matches( newPattern( filter.getKey(),
                caseInsensitiveFilters ), key, caseInsensitiveFilters,
                looseFilters ) )
            {
                String filterValue = filter.getValue() != null ?
                    filter.getValue().toString() : null;
                if ( matches( newPattern( filterValue,
                    caseInsensitiveFilters ), value.toString(),
                    caseInsensitiveFilters, looseFilters ) )
                {
                    return true;
                }
            }
        }
        return false;
    }

    protected static String frame( String string, boolean frame )
    {
        return frame ? "[" + string + "]" : string;
    }

    protected static String format( Object value, boolean includeFraming )
    {
        String result;
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
            boolean forcePrintHitHeader, boolean newLineBetweenHits, NodeOrRelationship entity,
            GraphDatabaseShellServer server, Session session, Output out )
            throws ShellException, RemoteException
    {
        if ( templateLines.isEmpty() || forcePrintHitHeader )
        {
            out.println( getDisplayName( server, session, entity, true ) );
        }

        if ( !templateLines.isEmpty() )
        {
            Map<String, Object> data = new HashMap<String, Object>();
            data.put( "i", entity.getId() );
            for ( String command : templateLines )
            {
                String line = TextUtil.templateString( command, data );
                server.interpretLine( session.getId(), line, out );
            }
        }
        if ( newLineBetweenHits )
        {
            out.println();
        }
    }

    /**
     * Reads the session variable specified in {@link org.neo4j.shell.Variables#WORKING_DIR_KEY} and
     * returns it as a list of typed ids.
     * @param session the session to read from.
     * @return the working directory as a list.
     * @throws RemoteException if an RMI error occurs.
     */
    public static List<TypedId> readCurrentWorkingDir( Session session ) throws RemoteException
    {
        List<TypedId> list = new ArrayList<TypedId>();
        String path = session.getPath();
        if ( path != null && path.trim().length() > 0 )
        {
            for ( String typedId : path.split( "," ) )
            {
                list.add( new TypedId( typedId ) );
            }
        }
        return list;
    }

    public static void writeCurrentWorkingDir( List<TypedId> paths, Session session ) throws RemoteException
    {
        String path = makePath( paths );
        session.setPath( path );
    }

    private static String makePath( List<TypedId> paths )
    {
        StringBuilder buffer = new StringBuilder();
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

    protected static Map<String, Direction> filterMapToTypes( GraphDatabaseService db,
            Direction defaultDirection, Map<String, Object> filterMap, boolean caseInsensitiveFilters,
            boolean looseFilters ) throws ShellException
    {
        Map<String, Direction> matches = new TreeMap<String, Direction>();
        for ( RelationshipType type : GlobalGraphOperations.at( db ).getAllRelationshipTypes() )
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
                matches.put( type.name(), direction );
            }
        }
        return matches.isEmpty() ? Collections.<String, Direction>emptyMap() : matches;
    }

    protected static PathExpander toExpander( GraphDatabaseService db, Direction defaultDirection,
            Map<String, Object> relationshipTypes, boolean caseInsensitiveFilters, boolean looseFilters ) throws ShellException
    {
        defaultDirection = defaultDirection != null ? defaultDirection : Direction.BOTH;
        Map<String, Direction> matches = filterMapToTypes( db, defaultDirection, relationshipTypes,
                caseInsensitiveFilters, looseFilters );
        Expander expander = Traversal.emptyExpander();
        if ( matches == null )
        {
            return EMPTY_EXPANDER;
        }
        for ( Map.Entry<String, Direction> entry : matches.entrySet() )
        {
            expander = expander.add( DynamicRelationshipType.withName( entry.getKey() ),
                    entry.getValue() );
        }
        return (PathExpander) expander;
    }

    protected static PathExpander toSortedExpander( GraphDatabaseService db, Direction defaultDirection,
            Map<String, Object> relationshipTypes, boolean caseInsensitiveFilters, boolean looseFilters ) throws ShellException
    {
        defaultDirection = defaultDirection != null ? defaultDirection : Direction.BOTH;
        Map<String, Direction> matches = filterMapToTypes( db, defaultDirection, relationshipTypes,
                caseInsensitiveFilters, looseFilters );
        Expander expander = new OrderedByTypeExpander();
        for ( Map.Entry<String, Direction> entry : matches.entrySet() )
        {
            expander = expander.add( DynamicRelationshipType.withName( entry.getKey() ),
                    entry.getValue() );
        }
        return (PathExpander) expander;
    }

    private static final PathExpander EMPTY_EXPANDER = new PathExpander()
    {
        @Override
        public PathExpander reverse()
        {
            return this;
        }

        @Override
        public Iterable<Relationship> expand( Path path, BranchState state )
        {
            return Collections.emptyList();
        }
    };

    protected Label[] parseLabels( AppCommandParser parser )
    {
        return parseValues( parser, "l", EMPTY_LABELS, CREATE_LABELS );
    }

    protected RelationshipType[] parseRelTypes( AppCommandParser parser )
    {
        return parseValues( parser, "r", EMPTY_REL_TYPES, CREATE_REL_TYPES );
    }

    protected <T> T[] parseValues( AppCommandParser parser, String opt, T[] emptyValue, Function<String[],T[]> factory )
    {
        String typeValue = parser.option( opt, null );
        if ( typeValue == null )
        {
            return emptyValue;
        }
        typeValue = typeValue.trim();

        String[] stringItems;
        if ( typeValue.startsWith( "[" ) )
        {
            Object[] items = parseArray( typeValue );
            stringItems = new String[items.length];
            for ( int i = 0; i < items.length; i++ )
            {
                stringItems[i] = withOrWithoutColon( items[i].toString() );
            }
        }
        else
        {
            stringItems = new String[]{withOrWithoutColon( typeValue )};
        }
        return factory.apply( stringItems );
    }

    private String withOrWithoutColon( String item )
    {
        return item.startsWith( ":" ) ? item.substring( 1 ) : item;
    }
}
