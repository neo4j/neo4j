/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.commandline.arguments;

import org.apache.commons.text.WordUtils;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.arguments.common.Database;
import org.neo4j.commandline.arguments.common.MandatoryCanonicalPath;
import org.neo4j.commandline.arguments.common.OptionalCanonicalPath;
import org.neo4j.helpers.Args;

import static java.lang.String.format;

/**
 * Builder class for constructing a suitable arguments-string for displaying in help messages and alike.
 * Some common arguments have convenience functions.
 */
public class Arguments
{
    public static final Arguments NO_ARGS = new Arguments();
    private static final int LINE_LENGTH = 80;
    private static final int MIN_RIGHT_COL_WIDTH = 30;
    private static final String NEWLINE = System.getProperty( "line.separator" );
    private final Map<String,NamedArgument> namedArgs;
    private final ArrayList<PositionalArgument> positionalArgs;
    private Args parsedArgs;

    public Arguments()
    {
        namedArgs = new LinkedHashMap<>();
        positionalArgs = new ArrayList<>();
    }

    public Arguments withDatabase()
    {
        return withArgument( new Database() );
    }

    public Arguments withDatabase( String description )
    {
        return withArgument( new Database( description ) );
    }

    public Arguments withAdditionalConfig()
    {
        return withArgument( new OptionalCanonicalPath( "additional-config", "config-file-path", "",
                "Configuration file to supply additional configuration in." ) );
    }

    public Arguments withTo( String description )
    {
        return withArgument( new MandatoryCanonicalPath( "to", "destination-path", description ) );
    }

    public Arguments withOptionalPositionalArgument( int position, String value )
    {
        return withPositionalArgument( new OptionalPositionalArgument( position, value ) );
    }

    public Arguments withMandatoryPositionalArgument( int position, String value )
    {
        return withPositionalArgument( new MandatoryPositionalArgument( position, value ) );
    }

    public Arguments withArgument( NamedArgument namedArgument )
    {
        namedArgs.put( namedArgument.name(), namedArgument );
        return this;
    }

    public Arguments withPositionalArgument( PositionalArgument arg )
    {
        positionalArgs.add( arg );
        return this;
    }

    public String usage()
    {
        StringBuilder sb = new StringBuilder();

        if ( !namedArgs.isEmpty() )
        {
            sb.append( namedArgs.values().stream().map( NamedArgument::usage ).collect( Collectors.joining( " " ) ) );
        }

        if ( !positionalArgs.isEmpty() )
        {
            sb.append( " " );
            positionalArgs.sort( Comparator.comparingInt( PositionalArgument::position ) );
            sb.append( positionalArgs.stream().map( PositionalArgument::usage ).collect( Collectors.joining( " " ) ) );
        }

        return sb.toString().trim();
    }

    public String description( String text )
    {
        String wrappedText = wrapText( text, LINE_LENGTH );
        if ( namedArgs.isEmpty() )
        {
            return wrappedText;
        }

        wrappedText = String.join( NEWLINE + NEWLINE, wrappedText, "options:" );

        //noinspection OptionalGetWithoutIsPresent handled by if-statement above
        final int alignLength = namedArgs.values().stream()
                .map( a -> a.optionsListing().length() )
                .reduce( 0, Integer::max );

        return String.join( NEWLINE, wrappedText,
                namedArgs.values().stream()
                        .map( c -> formatArgumentDescription( alignLength, c ) )
                        .collect( Collectors.joining( NEWLINE ) ) );
    }

    /**
     * Original line-endings in the text are respected.
     *
     * @param text to wrap
     * @param lineLength no line will exceed this length
     * @return the text where no line exceeds the specified length
     */
    public static String wrapText( final String text, final int lineLength )
    {
        List<String> lines = Arrays.asList( text.split( "\r?\n" ) );

        return lines.stream()
                .map( l ->
                        l.length() > lineLength ? WordUtils.wrap( l, lineLength ) : l )
                .collect( Collectors.joining( NEWLINE ) );
    }

    public String formatArgumentDescription( final int longestAlignmentLength, final NamedArgument argument )
    {
        final String left = format( "  %s", argument.optionsListing() );
        final String right;
        if ( argument instanceof OptionalNamedArg )
        {
            right = format( "%s [default:%s]", argument.description(),
                    ((OptionalNamedArg) argument).defaultValue() );
        }
        else
        {
            right = argument.description();
        }
        // 5 = 2 leading spaces in left + 3 spaces as distance between columns
        return rightColumnFormatted( left, right, longestAlignmentLength + 5 );
    }

    public static String rightColumnFormatted( final String leftText, final String rightText, int rightAlignIndex )
    {
        final int newLineIndent = 6;
        int rightWidth = Arguments.LINE_LENGTH - rightAlignIndex;
        boolean startOnNewLine = false;
        if ( rightWidth < MIN_RIGHT_COL_WIDTH )
        {
            startOnNewLine = true;
            rightWidth = LINE_LENGTH - newLineIndent;
        }

        final String[] rightLines = wrapText( rightText, rightWidth ).split( NEWLINE );

        final String fmt = "%-" + (startOnNewLine ? newLineIndent : rightAlignIndex) + "s%s";
        String firstLine = format( fmt, leftText, startOnNewLine ? "" : rightLines[0] );

        String rest = Arrays.stream( rightLines )
                .skip( startOnNewLine ? 0 : 1 )
                .map( l -> format( fmt, "", l ) )
                .collect( Collectors.joining( NEWLINE ) );

        if ( rest.isEmpty() )
        {
            return firstLine;
        }
        else
        {
            return String.join( NEWLINE, firstLine, rest );
        }
    }

    public Arguments parse( String[] args ) throws IncorrectUsage
    {
        // Get boolean flags
        List<String> flags = namedArgs.entrySet().stream()
                .filter( e -> e.getValue() instanceof OptionalBooleanArg )
                .map( Entry::getKey )
                .collect( Collectors.toList() );

        parsedArgs = Args.withFlags( flags.toArray( new String[flags.size()] ) ).parse( args );
        validate();
        return this;
    }

    public String get( int pos )
    {
        if ( pos >= 0 && pos < positionalArgs.size() )
        {
            return positionalArgs.get( pos ).parse( parsedArgs );
        }
        throw new IllegalArgumentException( format( "Positional argument '%d' not specified.", pos ) );
    }

    public String get( String argName )
    {
        if ( namedArgs.containsKey( argName ) )
        {
            return namedArgs.get( argName ).parse( parsedArgs );
        }
        throw new IllegalArgumentException( "No such argument available to be parsed: " + argName );
    }

    private void validate() throws IncorrectUsage
    {
        for ( String o : parsedArgs.asMap().keySet() )
        {
            if ( !namedArgs.containsKey( o ) )
            {
                throw new IncorrectUsage( format( "unrecognized option: '%s'", o ) );
            }
        }
        long mandatoryPositionalArgs = positionalArgs.stream()
                .filter( o -> o instanceof MandatoryPositionalArgument )
                .count();
        if ( parsedArgs.orphans().size() < mandatoryPositionalArgs )
        {
            throw new IncorrectUsage( "not enough arguments" );
        }
        String excessArgs = parsedArgs.orphans().stream()
                .skip( positionalArgs.size() )
                .collect( Collectors.joining( " " ) );
        if ( !excessArgs.isEmpty() )
        {
            throw new IncorrectUsage( format( "unrecognized arguments: '%s'", excessArgs ) );
        }
    }

    public boolean getBoolean( String argName )
    {
        return get( argName, Boolean::parseBoolean );
    }

    public Optional<Path> getOptionalPath( String argName )
    {
        String p = get( argName );

        if ( p.isEmpty() )
        {
            return Optional.empty();
        }

        return Optional.of( Paths.get( p ) );
    }

    public Path getMandatoryPath( String argName )
    {
        Optional<Path> p = getOptionalPath( argName );
        if ( p.isPresent() )
        {
            return p.get();
        }
        throw new IllegalArgumentException( format( "Missing exampleValue for '%s'", argName ) );
    }

    public <T> T get( String argName, Function<String,T> converter )
    {
        return converter.apply( get( argName ) );
    }

    /**
     * @param argName name of argument
     * @return true if argName was given as an explicit argument, false otherwise
     */
    public boolean has( String argName )
    {
        if ( namedArgs.containsKey( argName ) )
        {
            return namedArgs.get( argName ).has( parsedArgs );
        }
        throw new IllegalArgumentException( "No such argument available: " + argName );
    }
}
