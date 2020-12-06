/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.shell.log;

import org.fusesource.jansi.Ansi;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A piece of text which can be rendered with Ansi format codes.
 */
public class AnsiFormattedText
{

    private static final String RED = "RED";
    private static final String BOLD = "BOLD";
    private static final String DEFAULT_COLOR = "DEFAULT";
    // no mapping means not defined
    private final Map<String, Boolean> attributes = new HashMap<>();
    // Each piece is formatted separately
    private final LinkedList<AnsiFormattedString> pieces = new LinkedList<>();
    // can be defined, or undefined. null means undefined.
    private String color;

    /**
     * Return a new map which is a copy of the first map, with the keys/values from the second if they do not override anything already defined in the first
     * map.
     */
    private static <K, V> Map<K, V> mergeMaps( Map<K, V> primary, Map<K, V> secondary )
    {
        Map<K, V> result = new HashMap<>();
        result.putAll( primary );
        secondary.forEach( result::putIfAbsent );
        return result;
    }

    /**
     * @return a new empty instance
     */
    public static AnsiFormattedText s()
    {
        return new AnsiFormattedText();
    }

    /**
     * @param string to start with, may be null in which case it is ignored
     * @return a new instance containing the unformatted text in string, or empty if it was null
     */
    public static AnsiFormattedText from( @Nullable String string )
    {
        AnsiFormattedText st = new AnsiFormattedText();
        if ( string != null )
        {
            st.append( string );
        }
        return st;
    }

    /**
     * @return the text as a string including possible formatting, ready for ANSI formatting
     */
    @Nonnull
    public String formattedString()
    {
        StringBuilder sb = new StringBuilder();
        for ( AnsiFormattedString s : pieces )
        {
            List<String> codes = new ArrayList<>();

            // color
            if ( s.color != null && !DEFAULT_COLOR.equals( s.color ) )
            {
                codes.add( s.color );
            }
            // attributes
            if ( s.attributes.getOrDefault( BOLD, false ) )
            {
                codes.add( BOLD );
            }
            // Only do formatting if we actually have some formatting to apply
            if ( !codes.isEmpty() )
            {
                sb.append( "@|" )
                  .append( String.join( ",", codes ) )
                  .append( " " );
            }
            // string
            sb.append( s.string );
            // Only reset formatting if we actually did some formatting
            if ( !codes.isEmpty() )
            {
                sb.append( "|@" );
            }
        }
        return sb.toString();
    }

    /**
     * @return the text as a string rendered with ANSI escape codes
     */
    @Nonnull
    public String renderedString()
    {
        return Ansi.ansi().render( formattedString() ).toString();
    }

    /**
     * @return the text as a plain string without any formatting
     */
    @Nonnull
    public String plainString()
    {
        StringBuilder sb = new StringBuilder();
        pieces.forEach( sb::append );
        return sb.toString();
    }

    /**
     * Append an already formatted string. If any formatting codes are defined, then they will be ignored in favor of this instance's formatting.
     *
     * @param existing text to append using this instance's formatting
     * @return this
     */
    public AnsiFormattedText append( AnsiFormattedText existing )
    {
        existing.pieces.forEach( s -> pieces.add( new AnsiFormattedString( color != null ? color : s.color,
                                                                           mergeMaps( attributes, s.attributes ), s.string ) ) );
        return this;
    }

    /**
     * Append a string using the current formatting
     *
     * @param s string to append using this instance's formatting
     * @return this
     */
    public AnsiFormattedText append( String s )
    {
        pieces.add( new AnsiFormattedString( color, attributes, s ) );
        return this;
    }

    /**
     * Append a new line
     *
     * @return this
     */
    public AnsiFormattedText appendNewLine()
    {
        pieces.add( new AnsiFormattedString( color, attributes, System.lineSeparator() ) );
        return this;
    }

    /**
     * Set formatting to bold. Note that this has no effect on strings already in the text.
     *
     * @return this
     */
    public AnsiFormattedText bold()
    {
        attributes.put( BOLD, true );
        return this;
    }

    /**
     * Set formatting to not bold. Note that this has no effect on strings already in the text.
     *
     * @return this
     */
    public AnsiFormattedText boldOff()
    {
        attributes.put( BOLD, false );
        return this;
    }

    /**
     * Set color to red. Note that this has no effect on strings already in the text.
     *
     * @return this
     */
    public AnsiFormattedText colorRed()
    {
        color = RED;
        return this;
    }

    /**
     * Set color to default. Note that this has no effect on strings already in the text.
     *
     * @return this
     */
    public AnsiFormattedText colorDefault()
    {
        color = DEFAULT_COLOR;
        return this;
    }

    /**
     * A formatted string
     */
    private static class AnsiFormattedString
    {
        // can be defined, or undefined. null means undefined.
        final String color;
        // same here, no mapping means undefined
        final Map<String, Boolean> attributes = new HashMap<>();
        final String string;

        AnsiFormattedString( String color, Map<String, Boolean> attributes, String s )
        {
            this.color = color;
            this.attributes.putAll( attributes );
            this.string = s;
        }

        @Override
        public String toString()
        {
            return string;
        }
    }
}
