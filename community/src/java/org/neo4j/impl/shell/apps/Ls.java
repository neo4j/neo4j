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
import java.util.regex.Pattern;

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
            "Filters property keys/relationship types (regexp string)" ) );
        this.addValueType( "g", new OptionContext( OptionValueType.MUST,
            "Filters property values (regexp string)" ) );
        this.addValueType( "s", new OptionContext( OptionValueType.NONE,
            "Case sensitive filters" ) );
        this.addValueType( "x", new OptionContext( OptionValueType.NONE,
            "Filters will only match if the entire value matches " +
            "(exact match)" ) );
        this.addValueType( "e", new OptionContext( OptionValueType.MUST,
            "Temporarily select a connected relationship to do the "
                + "operation on" ) );
    }

    @Override
    public String getDescription()
    {
        return "Lists the current node. Optionally supply node id for "
            + "listing a certain node: ls <node-id>";
    }

    @Override
    protected String exec( AppCommandParser parser, Session session, Output out )
        throws ShellException, RemoteException
    {
        boolean verbose = parser.options().containsKey( "v" );
        boolean displayValues = verbose || !parser.options().containsKey( "q" );
        boolean displayProperties = parser.options().containsKey( "p" );
        boolean displayRelationships = parser.options().containsKey( "r" );
        boolean caseSensitiveFilters = parser.options().containsKey( "s" );
        boolean exactFilterMatch = parser.options().containsKey( "x" );
        String keyFilter = parser.options().get( "f" );
        String valueFilter = parser.options().get( "g" );
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
                keyFilter, valueFilter, caseSensitiveFilters,
                exactFilterMatch );
        }
        if ( displayRelationships )
        {
            this.displayRelationships( parser, thing, out,
                verbose, keyFilter, caseSensitiveFilters, exactFilterMatch );
        }
        return null;
    }

    private void displayProperties( NodeOrRelationship thing, Output out,
        boolean displayValues, boolean verbose, String keyFilter,
        String valueFilter, boolean caseSensitiveFilters,
        boolean exactFilterMatch ) throws RemoteException
    {
        int longestKey = this.findLongestKey( thing );
        Pattern keyPattern = newPattern( keyFilter, caseSensitiveFilters );
        Pattern valuePattern = newPattern( valueFilter, caseSensitiveFilters );
        for ( String key : thing.getPropertyKeys() )
        {
            if ( !matches( keyPattern, key, caseSensitiveFilters,
                exactFilterMatch ) )
            {
                continue;
            }
            Object value = thing.getProperty( key );
            if ( !matches( valuePattern, value.toString(),
                caseSensitiveFilters, exactFilterMatch ) )
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
    
    private String fixCaseSensitivity( String string,
        boolean caseSensitive )
    {
        return caseSensitive ? string : string.toLowerCase();
    }
    
    private Pattern newPattern( String pattern, boolean caseSensitive )
    {
        return pattern == null ? null : Pattern.compile(
            fixCaseSensitivity( pattern, caseSensitive ) );
    }

    private void displayRelationships( AppCommandParser parser,
        NodeOrRelationship thing, Output out,
        boolean verbose, String filter, boolean caseSensitiveFilters,
        boolean exactFilterMatch )
        throws ShellException, RemoteException
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
                "--[", "]-->", filter, caseSensitiveFilters, exactFilterMatch );
        }
        if ( displayIncoming )
        {
            displayRelationships( thing, out, verbose, Direction.INCOMING,
                "<--[", "]--", filter, caseSensitiveFilters, exactFilterMatch );
        }
    }
    
    private boolean matches( Pattern patternOrNull, String value,
        boolean caseSensitive, boolean exactMatch )
    {
        if ( patternOrNull == null )
        {
            return true;
        }
        
        value = fixCaseSensitivity( value, caseSensitive );
        return exactMatch ?
            patternOrNull.matcher( value ).matches() :
            patternOrNull.matcher( value ).find();
    }
    
    private void displayRelationships( NodeOrRelationship thing,
        Output out, boolean verbose, Direction direction, String prefixString,
        String postfixString, String filter, boolean caseSensitiveFilters,
        boolean exactFilterMatch ) throws ShellException, RemoteException
    {
        Pattern typeFilter = newPattern( filter, caseSensitiveFilters );
        for ( Relationship rel : thing.getRelationships( direction ) )
        {
            String type = rel.getType().name();
            if ( !matches( typeFilter, type, caseSensitiveFilters,
                exactFilterMatch ) )
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