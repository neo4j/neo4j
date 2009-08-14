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

import java.lang.reflect.Array;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.neo4j.api.core.Direction;
import org.neo4j.api.core.Relationship;
import org.neo4j.impl.shell.NeoApp;
import org.neo4j.util.shell.AppCommandParser;
import org.neo4j.util.shell.OptionValueType;
import org.neo4j.util.shell.Output;
import org.neo4j.util.shell.Session;
import org.neo4j.util.shell.ShellException;

/**
 * Mimics the POSIX application with the same name, i.e. lists
 * properties/relationships on a node or a relationship.
 */
public class Ls extends NeoApp
{
    /**
     * Constructs a new "ls" application.
     */
    public Ls()
    {
        super();
        this.addValueType( "d", new OptionContext( OptionValueType.MUST,
            "Direction filter for relationships: "
                + this.directionAlternatives() ) );
        this.addValueType( "v", new OptionContext( OptionValueType.NONE,
            "Verbose mode" ) );
        this.addValueType( "q", new OptionContext( OptionValueType.NONE,
            "Quiet mode" ) );
        this.addValueType( "p", new OptionContext( OptionValueType.NONE,
            "Lists properties" ) );
        this.addValueType( "r", new OptionContext( OptionValueType.NONE,
            "Lists relationships" ) );
        this.addValueType( "f", new OptionContext( OptionValueType.MUST,
            "Filters node property keys/values. Supplied either as a single " +
            "value\n" +
            "or as a JSON string where both keys and values can " +
            "contain regex.\n" +
            "Starting/ending {} brackets are optional. Examples:\n" +
            "\"username\"\n" +
            "   property/relationship 'username' gets listed\n" +
            "\".*name: ma.*, age: ''\"\n" +
            "   properties with keys matching '.*name' and values matching " +
            "'ma.*'\n" +
            "   gets listed, as well as the 'age' property. Also " +
            "relationships\n" +
            "   matching '.*name' or 'age' gets listed" ) );
        this.addValueType( "i", new OptionContext( OptionValueType.NONE,
            "Filters are case-insensitive (case-sensitive by default)" ) );
        this.addValueType( "l", new OptionContext( OptionValueType.NONE,
            "Filters matches more loosely, i.e. it's considered a match if " +
            "just\n" +
            "a part of a value matches the pattern, not necessarily " +
            "the whole value" ) );
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
        boolean verbose = parser.options().containsKey( "v" );
        boolean displayValues = verbose || !parser.options().containsKey( "q" );
        boolean displayProperties = parser.options().containsKey( "p" );
        boolean displayRelationships = parser.options().containsKey( "r" );
        boolean caseInsensitiveFilters = parser.options().containsKey( "i" );
        boolean looseFilters = parser.options().containsKey( "l" );
        String filterString = parser.options().get( "f" );
        Map<String, Object> filterMap = parseFilter( filterString, out );
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
            this.displayProperties( thing, out, displayValues, verbose,
                filterMap, caseInsensitiveFilters, looseFilters );
        }
        if ( displayRelationships )
        {
            this.displayRelationships( parser, thing, session, out,
                verbose, filterMap, caseInsensitiveFilters, looseFilters );
        }
        return null;
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
    
    private Iterable<Relationship> sortRelationships(
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
        
        Collection<Relationship> result = new ArrayList<Relationship>();
        for ( Collection<Relationship> rels : map.values() )
        {
            result.addAll( rels );
        }
        return result;
    }

    private void displayProperties( NodeOrRelationship thing, Output out,
        boolean displayValues, boolean verbose, Map<String, Object> filterMap,
        boolean caseInsensitiveFilters, boolean looseFilters )
        throws RemoteException
    {
        int longestKey = findLongestKey( thing );
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
            
            out.print( "*" + key );
            if ( displayValues )
            {
                this.printMany( out, " ", longestKey - key.length() + 1 );
                out.print( "=" + format( value, true ) );
                if ( verbose )
                {
                    out.print( " (" + getNiceType( value ) + ")" );
                }
            }
            out.println( "" );
        }
    }
    
    public static String format( Object value, boolean includeFraming )
    {
        String result = null;
        if ( value.getClass().isArray() )
        {
            StringBuffer buffer = new StringBuffer();
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
    
    public static String frame( String string, boolean frame )
    {
        return frame ? "[" + string + "]" : string;
    }

    private void displayRelationships( AppCommandParser parser,
        NodeOrRelationship thing, Session session, Output out, boolean verbose,
        Map<String, Object> filterMap, boolean caseInsensitiveFilters,
        boolean looseFilters ) throws ShellException, RemoteException
    {
        String directionFilter = parser.options().get( "d" );
        Direction direction = this.getDirection( directionFilter );
        boolean displayOutgoing = directionFilter == null
            || direction == Direction.OUTGOING;
        boolean displayIncoming = directionFilter == null
            || direction == Direction.INCOMING;
        if ( displayOutgoing )
        {
            displayRelationships( thing, session, out, verbose,
                Direction.OUTGOING, "--", "-->", filterMap,
                caseInsensitiveFilters, looseFilters );
        }
        if ( displayIncoming )
        {
            displayRelationships( thing, session, out, verbose,
                Direction.INCOMING, "<--", "--", filterMap,
                caseInsensitiveFilters, looseFilters );
        }
    }
    
    private void displayRelationships( NodeOrRelationship thing,
        Session session, Output out, boolean verbose, Direction direction,
        String prefixString, String postfixString,
        Map<String, Object> filterMap, boolean caseInsensitiveFilters,
        boolean looseFilters )
        throws ShellException, RemoteException
    {
        for ( Relationship rel : sortRelationships(
            thing.getRelationships( direction ) ) )
        {
            String type = rel.getType().name();
            boolean matches = filterMap.isEmpty();
            for ( String filter : filterMap.keySet() )
            {
                if ( matches( newPattern( filter, caseInsensitiveFilters ),
                    type, caseInsensitiveFilters, looseFilters ) )
                {
                    matches = true;
                    break;
                }
            }
            
            if ( !matches )
            {
                continue;
            }
            
            StringBuffer buf = new StringBuffer(
                getDisplayNameForCurrent( session ) );
            buf.append( " " + prefixString ).append( getDisplayName(
                getNeoServer(), session, rel, verbose ) );
            buf.append( postfixString + " " );
            buf.append( getDisplayName( getNeoServer(), session,
                direction == Direction.OUTGOING ? rel.getEndNode() :
                    rel.getStartNode() ) );
            out.println( buf );
        }
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