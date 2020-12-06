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
package org.neo4j.shell.prettyprint;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.summary.Plan;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;
import static org.neo4j.shell.prettyprint.OutputFormatter.NEWLINE;
import static org.neo4j.shell.prettyprint.OutputFormatter.repeat;

public class TablePlanFormatter
{

    public static final String IDENTIFIERS = "Identifiers";
    public static final String DETAILS = "Details";
    public static final int MAX_DETAILS_COLUMN_WIDTH = 100;
    public static final Value ZERO_VALUE = Values.value( 0 );
    private static final String UNNAMED_PATTERN_STRING = "  (UNNAMED|FRESHID|AGGREGATION|NODE|REL)(\\d+)";
    private static final Pattern UNNAMED_PATTERN = Pattern.compile( UNNAMED_PATTERN_STRING );
    private static final String OPERATOR = "Operator";
    private static final String ESTIMATED_ROWS = "Estimated Rows";
    private static final String ROWS = "Rows";
    private static final String HITS = "DB Hits";
    private static final String PAGE_CACHE = "Page Cache Hits/Misses";
    private static final String TIME = "Time (ms)";
    private static final String ORDER = "Ordered by";
    private static final String MEMORY = "Memory (Bytes)";
    private static final String OTHER = "Other";
    private static final String SEPARATOR = ", ";
    private static final Pattern DEDUP_PATTERN = Pattern.compile( "\\s*(\\S+)@\\d+" );
    private static final List<String> HEADERS = asList( OPERATOR, DETAILS, ESTIMATED_ROWS, ROWS, HITS, MEMORY, PAGE_CACHE, TIME, IDENTIFIERS, ORDER, OTHER );
    private static final Set<String> IGNORED_ARGUMENTS = new LinkedHashSet<>(
            asList( "Rows", "DbHits", "EstimatedRows", "planner", "planner-impl", "planner-version", "version", "runtime", "runtime-impl", "runtime-version",
                    "Time", "time", "source-code", "PageCacheMisses", "PageCacheHits", "PageCacheHitRatio", "Order", "Memory", "GlobalMemory", "Details" ) );

    private static void pad( int width, char chr, @Nonnull StringBuilder result )
    {
        result.append( OutputFormatter.repeat( chr, width ) );
    }

    private int width( @Nonnull String header, @Nonnull Map<String, Integer> columns )
    {
        return 2 + Math.max( header.length(), columns.get( header ) );
    }

    private void divider( @Nonnull List<String> headers, @Nullable TableRow tableRow /*= null*/, @Nonnull StringBuilder result,
                          @Nonnull Map<String, Integer> columns )
    {
        for ( String header : headers )
        {
            if ( tableRow != null && header.equals( OPERATOR ) && tableRow.connection.isPresent() )
            {
                result.append( "|" );
                String connection = tableRow.connection.get();
                result.append( " " ).append( connection );
                pad( width( header, columns ) - connection.length() - 1, ' ', result );
            }
            else
            {
                result.append( "+" );
                pad( width( header, columns ), '-', result );
            }
        }
        result.append( "+" ).append( NEWLINE );
    }

    @Nonnull
    String formatPlan( @Nonnull Plan plan )
    {
        Map<String, Integer> columns = new HashMap<>();
        List<TableRow> tableRows = accumulate( plan, new Root(), columns );

        // Remove Identifiers column if we have a Details column
        List<String> headers =
                HEADERS.stream().filter( header -> columns.containsKey( header ) && !(header.equals( IDENTIFIERS ) && columns.containsKey( DETAILS )) )
                       .collect( Collectors.toList() );

        StringBuilder result =
                new StringBuilder( (2 + NEWLINE.length() + headers.stream().mapToInt( h -> width( h, columns ) ).sum()) * (tableRows.size() * 2 + 3) );

        List<TableRow> allTableRows = new ArrayList<>();
        Map<String, Cell> headerMap =
                headers.stream().map( header -> Pair.of( header, new LeftJustifiedCell( header ) ) ).collect( toMap( p -> p._1, p -> p._2 ) );
        allTableRows.add( new TableRow( OPERATOR, headerMap, Optional.empty() ) );
        allTableRows.addAll( tableRows );
        for ( int rowIndex = 0; rowIndex < allTableRows.size(); rowIndex++ )
        {
            TableRow tableRow = allTableRows.get( rowIndex );
            divider( headers, tableRow, result, columns );
            for ( int rowLineIndex = 0; rowLineIndex < tableRow.height; rowLineIndex++ )
            {
                for ( String header : headers )
                {
                    Cell cell = tableRow.get( header );
                    String defaultText = "";
                    if ( header.equals( OPERATOR ) && rowIndex + 1 < allTableRows.size() )
                    {
                        defaultText = allTableRows.get( rowIndex + 1 ).connection.orElse( "" ).replace( '\\', ' ' );
                    }
                    result.append( "| " );
                    int columnWidth = width( header, columns );
                    cell.writePaddedLine( rowLineIndex, defaultText, columnWidth, result );
                    result.append( " " );
                }
                result.append( "|" ).append( NEWLINE );
            }
        }
        divider( headers, null, result, columns );

        return result.toString();
    }

    @Nonnull
    private String serialize( @Nonnull String key, @Nonnull Value v )
    {
        switch ( key )
        {
        case "ColumnsLeft":
            return removeGeneratedNames( v.asString() );
        case "LegacyExpression":
            return removeGeneratedNames( v.asString() );
        case "Expression":
            return removeGeneratedNames( v.asString() );
        case "UpdateActionName":
            return v.asString();
        case "LegacyIndex":
            return v.toString();
        case "version":
            return v.toString();
        case "planner":
            return v.toString();
        case "planner-impl":
            return v.toString();
        case "runtime":
            return v.toString();
        case "runtime-impl":
            return v.toString();
        case "MergePattern":
            return "MergePattern(" + v.toString() + ")";
        case "DbHits":
            return v.asNumber().toString();
        case "Rows":
            return v.asNumber().toString();
        case "Time":
            return v.asNumber().toString();
        case "EstimatedRows":
            return v.asNumber().toString();
        case "LabelName":
            return v.asString();
        case "KeyNames":
            return removeGeneratedNames( v.asString() );
        case "KeyExpressions":
            return String.join( SEPARATOR, v.asList( Value::asString ) );

        case "ExpandExpression":
            return removeGeneratedNames( v.asString() );
        case "Index":
            return v.asString();
        case "PrefixIndex":
            return v.asString();
        case "InequalityIndex":
            return v.asString();
        case "EntityByIdRhs":
            return v.asString();
        case "PageCacheMisses":
            return v.asNumber().toString();
        case "Details":
            return v.asString();
        default:
            return v.asObject().toString();
        }
    }

    @Nonnull
    private Stream<List<TableRow>> children( @Nonnull Plan plan, Level level, @Nonnull Map<String, Integer> columns )
    {
        List<? extends Plan> c = plan.children();
        switch ( c.size() )
        {
        case 0:
            return Stream.empty();
        case 1:
            return Stream.of( accumulate( c.get( 0 ), level.child(), columns ) );
        case 2:
            return Stream.of( accumulate( c.get( 1 ), level.fork(), columns ), accumulate( c.get( 0 ), level.child(), columns ) );
        default:
            throw new IllegalStateException( "Plan has more than 2 children " + c );
        }
    }

    @Nonnull
    private List<TableRow> accumulate( @Nonnull Plan plan, @Nonnull Level level, @Nonnull Map<String, Integer> columns )
    {
        String line = level.line() + plan.operatorType(); // wa plan.name
        mapping( OPERATOR, new LeftJustifiedCell( line ), columns );

        return Stream.concat(
                Stream.of( new TableRow( line, details( plan, columns ), level.connector() ) ),
                children( plan, level, columns ).flatMap( Collection::stream ) )
                     .collect( Collectors.toList() );
    }

    @Nonnull
    private Map<String, Cell> details( @Nonnull Plan plan, @Nonnull Map<String, Integer> columns )
    {
        Map<String, Value> args = plan.arguments();

        Stream<Optional<Pair<String, Cell>>> formattedPlan = args.entrySet().stream()
                                                                 .map( e ->
                                                                       {
                                                                           Value value = e.getValue();
                                                                           switch ( e.getKey() )
                                                                           {
                                                                           case "EstimatedRows":
                                                                               return mapping( ESTIMATED_ROWS, new RightJustifiedCell(
                                                                                       format( value.asDouble() ) ), columns );
                                                                           case "Rows":
                                                                               return mapping( ROWS, new RightJustifiedCell(
                                                                                       value.asNumber().toString() ), columns );
                                                                           case "DbHits":
                                                                               return mapping( HITS, new RightJustifiedCell(
                                                                                       value.asNumber().toString() ), columns );
                                                                           case "PageCacheHits":
                                                                               return mapping( PAGE_CACHE, new RightJustifiedCell(
                                                                                       String.format( "%s/%s", value.asNumber(),
                                                                                                      args.getOrDefault( "PageCacheMisses",
                                                                                                                         ZERO_VALUE )
                                                                                                          .asNumber() ) ), columns );
                                                                           case "Time":
                                                                               return mapping( TIME, new RightJustifiedCell(
                                                                                                       String.format( "%.3f",
                                                                                                                      value.asLong() / 1000000.0d ) ),
                                                                                               columns );
                                                                           case "Order":
                                                                               return mapping( ORDER, new LeftJustifiedCell(
                                                                                       String.format( "%s", value.asString() ) ), columns );
                                                                           case "Details":
                                                                               return mapping( DETAILS, new LeftJustifiedCell(
                                                                                       splitDetails( value.asString() ) ), columns );
                                                                           case "Memory":
                                                                               return mapping( MEMORY, new RightJustifiedCell(
                                                                                                       String.format( "%s", value.asNumber().toString() ) ),
                                                                                               columns );
                                                                           default:
                                                                               return Optional.empty();
                                                                           }
                                                                       } );

        return Stream.concat(
                formattedPlan,
                Stream.of(
                        Optional.of( Pair.of( IDENTIFIERS, new LeftJustifiedCell( identifiers( plan, columns ) ) ) ),
                        Optional.of( Pair.of( OTHER, new LeftJustifiedCell( other( plan, columns ) ) ) ) ) )
                     .filter( Optional::isPresent )
                     .collect( toMap( o -> o.get()._1, o -> o.get()._2 ) );
    }

    @Nonnull
    private Optional<Pair<String, Cell>> mapping( @Nonnull String key, @Nonnull Cell value, @Nonnull Map<String, Integer> columns )
    {
        update( columns, key, value.length );
        return Optional.of( Pair.of( key, value ) );
    }

    @Nonnull
    private String replaceAllIn( @Nonnull Pattern pattern, @Nonnull String s, @Nonnull Function<Matcher, String> mapper )
    {
        StringBuffer sb = new StringBuffer();
        Matcher matcher = pattern.matcher( s );
        while ( matcher.find() )
        {
            matcher.appendReplacement( sb, mapper.apply( matcher ) );
        }
        matcher.appendTail( sb );
        return sb.toString();
    }

    @Nonnull
    private String removeGeneratedNames( @Nonnull String s )
    {
        String named = replaceAllIn( UNNAMED_PATTERN, s, m -> "anon[" + m.group( 2 ) + "]" );
        return replaceAllIn( DEDUP_PATTERN, named, m -> m.group( 1 ) );
    }

    private void update( @Nonnull Map<String, Integer> columns, @Nonnull String key, int length )
    {
        columns.put( key, Math.max( columns.getOrDefault( key, 0 ), length ) );
    }

    @Nonnull
    private String identifiers( @Nonnull Plan description, @Nonnull Map<String, Integer> columns )
    {
        String result = description.identifiers().stream().map( this::removeGeneratedNames ).collect( joining( ", " ) );
        if ( !result.isEmpty() )
        {
            update( columns, IDENTIFIERS, result.length() );
        }
        return result;
    }

    @Nonnull
    private String other( @Nonnull Plan description, @Nonnull Map<String, Integer> columns )
    {
        String result = description.arguments().entrySet().stream().map( e ->
                                                                         {
                                                                             if ( !IGNORED_ARGUMENTS.contains( e.getKey() ) )
                                                                             {
                                                                                 return serialize( e.getKey(), e.getValue() );
                                                                             }
                                                                             return "";
                                                                         } ).filter( OutputFormatter::isNotBlank ).collect( Collectors.joining( "; " ) )
                                   .replaceAll( UNNAMED_PATTERN_STRING, "" );

        if ( !result.isEmpty() )
        {
            update( columns, OTHER, result.length() );
        }
        return result;
    }

    @Nonnull
    private String format( @Nonnull Double v )
    {
        if ( v.isNaN() )
        {
            return v.toString();
        }
        return String.valueOf( Math.round( v ) );
    }

    private String[] splitDetails( String original )
    {
        List<String> detailsList = new ArrayList<>();

        int currentPos = 0;
        while ( currentPos < original.length() )
        {
            int newPos = Math.min( original.length(), currentPos + MAX_DETAILS_COLUMN_WIDTH );
            detailsList.add( original.substring( currentPos, newPos ) );
            currentPos = newPos;
        }

        return detailsList.toArray( new String[0] );
    }

    static class TableRow
    {
        private final String tree;
        private final Map<String, Cell> cells;
        private final Optional<String> connection;
        private final int height;

        TableRow( String tree, Map<String, Cell> cells, Optional<String> connection )
        {
            this.tree = tree;
            this.cells = cells;
            this.connection = connection == null ? Optional.empty() : connection;
            this.height = cells.values().stream().mapToInt( v -> v.lines.length ).max().orElse( 0 );
        }

        Cell get( String key )
        {
            if ( key.equals( TablePlanFormatter.OPERATOR ) )
            {
                return new LeftJustifiedCell( tree );
            }
            else
            {
                return cells.getOrDefault( key, new LeftJustifiedCell( "" ) );
            }
        }
    }

    abstract static class Cell
    {
        final int length;
        final String[] lines;

        Cell( String[] lines )
        {
            this.length = Stream.of( lines ).mapToInt( String::length ).max().orElse( 0 );
            this.lines = lines;
        }

        abstract void writePaddedLine( int lineIndex, String orElseValue, int columnWidth, StringBuilder result );

        protected int paddingWidth( int columnWidth, String line )
        {
            return columnWidth - line.length() - 2;
        }

        protected String getLineOrElse( int lineIndex, String orElseValue )
        {
            if ( lineIndex < lines.length )
            {
                return lines[lineIndex];
            }
            else
            {
                return orElseValue;
            }
        }
    }

    static class LeftJustifiedCell extends Cell
    {
        LeftJustifiedCell( String... lines )
        {
            super( lines );
        }

        @Override
        void writePaddedLine( int lineIndex, String orElseValue, int columnWidth, StringBuilder result )
        {
            String line = getLineOrElse( lineIndex, orElseValue );
            result.append( line );
            pad( paddingWidth( columnWidth, line ), ' ', result );
        }
    }

    static class RightJustifiedCell extends Cell
    {
        RightJustifiedCell( String... lines )
        {
            super( lines );
        }

        @Override
        void writePaddedLine( int lineIndex, String orElseValue, int columnWidth, StringBuilder result )
        {
            String line = getLineOrElse( lineIndex, orElseValue );
            pad( paddingWidth( columnWidth, line ), ' ', result );
            result.append( line );
        }
    }

    abstract static class Level
    {
        abstract Level child();

        abstract Level fork();

        abstract String line();

        abstract Optional<String> connector();
    }

    static class Root extends Level
    {
        @Override
        Level child()
        {
            return new Child( 1 );
        }

        @Override
        Level fork()
        {
            return new Fork( 2 );
        }

        @Override
        String line()
        {
            return "+";
        }

        @Override
        Optional<String> connector()
        {
            return Optional.empty();
        }
    }

    static class Child extends Level
    {
        private final int level;

        Child( int level )
        {

            this.level = level;
        }

        @Override
        Level child()
        {
            return new Child( level );
        }

        @Override
        Level fork()
        {
            return new Fork( level + 1 );
        }

        @Override
        String line()
        {
            return repeat( "| ", level - 1 ) + "+";
        }

        @Override
        Optional<String> connector()
        {
            return Optional.of( repeat( "| ", level ) );
        }
    }

    static class Fork extends Level
    {
        private final int level;

        Fork( int level )
        {

            this.level = level;
        }

        @Override
        Level child()
        {
            return new Child( level );
        }

        @Override
        Level fork()
        {
            return new Fork( level + 1 );
        }

        @Override
        String line()
        {
            return repeat( "| ", level - 1 ) + "+";
        }

        @Override
        Optional<String> connector()
        {
            return Optional.of( repeat( "| ", level - 2 ) + "|\\" );
        }
    }

    static final class Pair<T1, T2>
    {
        final T1 _1;
        final T2 _2;

        private Pair( T1 _1, T2 _2 )
        {
            this._1 = _1;
            this._2 = _2;
        }

        public static <T1, T2> Pair<T1, T2> of( T1 _1, T2 _2 )
        {
            return new Pair<>( _1, _2 );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }

            Pair<?, ?> pair = (Pair<?, ?>) o;
            return _1.equals( pair._1 ) && _2.equals( pair._2 );
        }

        @Override
        public int hashCode()
        {
            return 31 * _1.hashCode() + _2.hashCode();
        }
    }
}
