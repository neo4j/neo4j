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
package org.neo4j.shell.kernel.apps;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Expander;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.TraversalFactory;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.OptionDefinition;
import org.neo4j.shell.OptionValueType;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;

/**
 * Mimics the POSIX application with the same name, i.e. lists
 * properties/relationships on a node or a relationship.
 */
public class Ls extends GraphDatabaseApp
{
    /**
     * Constructs a new "ls" application.
     */
    public Ls()
    {
        super();
        this.addOptionDefinition( "b", new OptionDefinition( OptionValueType.NONE,
            "Brief summary instead of full content" ) );
        this.addOptionDefinition( "d", new OptionDefinition( OptionValueType.MUST,
            "Direction filter for relationships: " + this.directionAlternatives() ) );
        this.addOptionDefinition( "v", new OptionDefinition( OptionValueType.NONE,
            "Verbose mode" ) );
        this.addOptionDefinition( "p", new OptionDefinition( OptionValueType.NONE,
            "Lists properties" ) );
        this.addOptionDefinition( "r", new OptionDefinition( OptionValueType.NONE,
            "Lists relationships" ) );
        this.addOptionDefinition( "f", new OptionDefinition( OptionValueType.MUST,
            "Filters property keys/values. Supplied either as a single value " +
            "or as a JSON string where both keys and values can contain regex. " +
            "Starting/ending {} brackets are optional. Examples:\n" +
            "\"username\"\n" +
            "   property/relationship 'username' gets listed\n" +
            "\".*name: ma.*, age: ''\"\n" +
            "   properties with keys matching '.*name' and values matching ma.*'\n" +
            "   gets listed, as well as the 'age' property. Also " +
            "relationships\n" + "  matching" +
            "  '.*name' or 'age' gets listed" ) );
        this.addOptionDefinition( "i", new OptionDefinition( OptionValueType.NONE,
            "Filters are case-insensitive (case-sensitive by default)" ) );
        this.addOptionDefinition( "l", new OptionDefinition( OptionValueType.NONE,
            "Filters matches more loosely, i.e. it's considered a match if just " +
            "a part of a value matches the pattern, not necessarily the whole value" ) );
    }

    @Override
    public String getDescription()
    {
        return "Lists the contents of the current node or relationship. " +
        	"Optionally supply\n" +
            "node id for listing a certain node using \"ls <node-id>\"";
    }

    @Override
    protected String exec( AppCommandParser parser, Session session,
        Output out ) throws ShellException, RemoteException
    {
        boolean brief = parser.options().containsKey( "b" );
        boolean verbose = parser.options().containsKey( "v" );
        boolean displayProperties = parser.options().containsKey( "p" );
        boolean displayRelationships = parser.options().containsKey( "r" );
        boolean caseInsensitiveFilters = parser.options().containsKey( "i" );
        boolean looseFilters = parser.options().containsKey( "l" );
        Map<String, Object> filterMap = parseFilter( parser.options().get( "f" ), out );
        if ( !displayProperties && !displayRelationships )
        {
            displayProperties = true;
            displayRelationships = true;
        }

        NodeOrRelationship thing = null;
        if ( parser.arguments().isEmpty() )
        {
            thing = this.getCurrent( session );
        }
        else
        {
            thing = NodeOrRelationship.wrap( this.getNodeById( Long
                .parseLong( parser.arguments().get( 0 ) ) ) );
        }

        if ( displayProperties )
        {
            displayProperties( thing, out, verbose, filterMap, caseInsensitiveFilters,
                    looseFilters, brief );
        }
        if ( displayRelationships )
        {
            if ( thing.isNode() )
            {
                displayRelationships( parser, thing, session, out, verbose, filterMap,
                        caseInsensitiveFilters, looseFilters, brief );
            }
            else
            {
                displayNodes( parser, thing, session, out );
            }
        }
        return null;
    }
    
    private void displayNodes( AppCommandParser parser, NodeOrRelationship thing,
            Session session, Output out ) throws RemoteException, ShellException
    {
        Relationship rel = thing.asRelationship();
        out.println( getDisplayName( getServer(), session, rel.getStartNode(), false ) +
                " --" + getDisplayName( getServer(), session, rel, true, false ) + "-> " +
                getDisplayName( getServer(), session, rel.getEndNode(), false ) );
    }

    private Iterable<String> sortKeys( Iterable<String> source )
    {
        List<String> list = new ArrayList<String>();
        for ( String item : source )
        {
            list.add( item );
        }
        Collections.sort( list, new Comparator<String>()
        {
            public int compare( String item1, String item2 )
            {
                return item1.toLowerCase().compareTo( item2.toLowerCase() );
            }
        } );
        return list;
    }

    private Map<String, Collection<Relationship>> readAllRelationships(
            Iterable<Relationship> source )
    {
        Map<String, Collection<Relationship>> map =
            new TreeMap<String, Collection<Relationship>>();
        for ( Relationship rel : source )
        {
            String type = rel.getType().name().toLowerCase();
            Collection<Relationship> rels = map.get( type );
            if ( rels == null )
            {
                rels = new ArrayList<Relationship>();
                map.put( type, rels );
            }
            rels.add( rel );
        }
        return map;
    }

    private void displayProperties( NodeOrRelationship thing, Output out,
        boolean verbose, Map<String, Object> filterMap,
        boolean caseInsensitiveFilters, boolean looseFilters, boolean brief )
        throws RemoteException
    {
        int longestKey = findLongestKey( thing );
        int count = 0;
        for ( String key : sortKeys( thing.getPropertyKeys() ) )
        {
            boolean matches = filterMap.isEmpty();
            Object value = thing.getProperty( key );
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
                        matches = true;
                        break;
                    }
                }
            }
            if ( !matches )
            {
                continue;
            }

            count++;
            if ( !brief )
            {
                out.print( "*" + key );
                this.printMany( out, " ", longestKey - key.length() + 1 );
                out.print( "=" + format( value, true ) );
                if ( verbose )
                {
                    out.print( " (" + getNiceType( value ) + ")" );
                }
                out.println( "" );
            }
        }
        if ( brief )
        {
            out.println( "Property count: " + count );
        }
    }

    private void displayRelationships( AppCommandParser parser,
        NodeOrRelationship thing, Session session, Output out, boolean verbose,
        Map<String, Object> filterMap, boolean caseInsensitiveFilters,
        boolean looseFilters, boolean brief ) throws ShellException, RemoteException
    {
        Direction direction = getDirection( parser.options().get( "d" ), Direction.BOTH );
        boolean displayOutgoing = direction == Direction.BOTH || direction == Direction.OUTGOING;
        boolean displayIncoming = direction == Direction.BOTH || direction == Direction.INCOMING;
        if ( displayOutgoing )
        {
            displayRelationships( thing, session, out, verbose,
                Direction.OUTGOING, "--", "->", filterMap,
                caseInsensitiveFilters, looseFilters, brief );
        }
        if ( displayIncoming )
        {
            displayRelationships( thing, session, out, verbose,
                Direction.INCOMING, "<-", "--", filterMap,
                caseInsensitiveFilters, looseFilters, brief );
        }
    }

    private void displayRelationships( NodeOrRelationship thing,
        Session session, Output out, boolean verbose, Direction direction,
        String prefixString, String postfixString,
        Map<String, Object> filterMap, boolean caseInsensitiveFilters,
        boolean looseFilters, boolean brief ) throws ShellException, RemoteException
    {
        RelationshipExpander expander = toExpander( direction, filterMap,
                caseInsensitiveFilters, looseFilters );
        Map<String, Collection<Relationship>> relationships =
                readAllRelationships( expander.expand( thing.asNode() ) );
        for ( Map.Entry<String, Collection<Relationship>> entry : relationships.entrySet() )
        {
            if ( brief )
            {
                out.println( getDisplayName( getServer(), session, thing, true ) +
                        " " + prefixString + getDisplayName( getServer(), session,
                                entry.getValue().iterator().next(), false, true ) +
                                postfixString + " x" + entry.getValue().size() );
            }
            else
            {
                for ( Relationship rel : entry.getValue() )
                {
                    StringBuffer buf = new StringBuffer( getDisplayName(
                        getServer(), session, thing, true ) );
                    buf.append( " " + prefixString ).append( getDisplayName(
                        getServer(), session, rel, verbose, true ) );
                    buf.append( postfixString + " " );
                    buf.append( getDisplayName( getServer(), session,
                        direction == Direction.OUTGOING ? rel.getEndNode() :
                            rel.getStartNode(), true ) );
                    out.println( buf );
                }
            }
        }
    }

    private RelationshipExpander toExpander( Direction direction,
            Map<String, Object> filterMap, boolean caseInsensitiveFilters,
            boolean looseFilters )
    {
        Expander expander = TraversalFactory.emptyExpander();
        for ( RelationshipType type : getServer().getDb().getRelationshipTypes() )
        {
            boolean matches = false;
            if ( filterMap == null || filterMap.isEmpty() )
            {
                matches = true;
            }
            else
            {
                for ( String filter : filterMap.keySet() )
                {
                    if ( matches( newPattern( filter, caseInsensitiveFilters ),
                        type.name(), caseInsensitiveFilters, looseFilters ) )
                    {
                        matches = true;
                        break;
                    }
                }
            }

            if ( matches )
            {
                expander = expander.add( type, direction );
            }
        }
        return expander;
    }

    private static String getNiceType( Object value )
    {
        return Set.getValueTypeName( value.getClass() );
    }

    private static int findLongestKey( NodeOrRelationship thing )
    {
        int length = 0;
        for ( String key : thing.getPropertyKeys() )
        {
            if ( key.length() > length )
            {
                length = key.length();
            }
        }
        return length;
    }
}