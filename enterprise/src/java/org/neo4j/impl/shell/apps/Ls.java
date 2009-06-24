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
package org.neo4j.impl.shell.apps;

import java.rmi.RemoteException;
import java.util.Map;

import org.neo4j.api.core.Direction;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.util.shell.AppCommandParser;
import org.neo4j.util.shell.OptionValueType;
import org.neo4j.util.shell.Output;
import org.neo4j.util.shell.Session;
import org.neo4j.util.shell.ShellException;

/**
 * Mimics the POSIX application with the same name, i.e. lists
 * properties/relationships on a node.
 */
public class Ls extends NodeOrRelationshipApp
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
        this.addValueType( "e", new OptionContext( OptionValueType.MUST,
            "Temporarily select a connected relationship to do " +
            "the operation on" ) );
    }

    @Override
    public String getDescription()
    {
        return "Lists the contents of the current node. Optionally supply " +
            "node id for listing\n" +
            "a certain node using \"ls <node-id>\"";
    }

    @Override
    protected String exec( AppCommandParser parser, Session session, Output out )
        throws ShellException, RemoteException
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

        Node node = null;
        if ( parser.arguments().isEmpty() )
        {
            node = this.getCurrentNode( session );
        }
        else
        {
            node = this.getNodeById( Long
                .parseLong( parser.arguments().get( 0 ) ) );
        }

        NodeOrRelationship thing = getNodeOrRelationship( node, parser );
        if ( displayProperties )
        {
            this.displayProperties( thing, out, displayValues, verbose,
                filterMap, caseInsensitiveFilters, looseFilters );
        }
        if ( displayRelationships )
        {
            this.displayRelationships( parser, thing, out,
                verbose, filterMap, caseInsensitiveFilters, looseFilters );
        }
        return null;
    }

    private void displayProperties( NodeOrRelationship thing, Output out,
        boolean displayValues, boolean verbose, Map<String, Object> filterMap,
        boolean caseInsensitiveFilters, boolean looseFilters )
        throws RemoteException
    {
        int longestKey = this.findLongestKey( thing );
        for ( String key : thing.getPropertyKeys() )
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
                out.print( "=[" + value + "]" );
                if ( verbose )
                {
                    out.print( " (" + this.getNiceType( value ) + ")" );
                }
            }
            out.println( "" );
        }
    }
    
    private void displayRelationships( AppCommandParser parser,
        NodeOrRelationship thing, Output out, boolean verbose,
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
            displayRelationships( thing, out, verbose, Direction.OUTGOING,
                "--[", "]-->", filterMap, caseInsensitiveFilters,
                looseFilters );
        }
        if ( displayIncoming )
        {
            displayRelationships( thing, out, verbose, Direction.INCOMING,
                "<--[", "]--", filterMap, caseInsensitiveFilters,
                looseFilters );
        }
    }
    
    private void displayRelationships( NodeOrRelationship thing,
        Output out, boolean verbose, Direction direction, String prefixString,
        String postfixString, Map<String, Object> filterMap,
        boolean caseInsensitiveFilters, boolean looseFilters )
        throws ShellException, RemoteException
    {
        for ( Relationship rel : thing.getRelationships( direction ) )
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
                getDisplayNameForCurrentNode() );
            buf.append( " " + prefixString ).append( rel.getType().name() );
            if ( verbose )
            {
                buf.append( ", " ).append( rel.getId() );
            }
            buf.append( postfixString + " " );
            buf.append( getDisplayNameForNode( direction == Direction.OUTGOING ?
                rel.getEndNode() : rel.getStartNode() ) );
            out.println( buf );
        }
    }

    private String getNiceType( Object value )
    {
        String cls = value.getClass().getName();
        return cls.substring(
            String.class.getPackage().getName().length() + 1 );
    }

    private int findLongestKey( NodeOrRelationship thing )
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