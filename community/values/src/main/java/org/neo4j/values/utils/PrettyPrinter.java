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
package org.neo4j.values.utils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;

import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.RelationshipValue;

/**
 * Pretty printer for AnyValues.
 * <p>
 * Used to format AnyValue as a json-like string, as following:
 * <ul>
 * <li>nodes: <code>(id=42 :LABEL {prop1: ["a", 13]})</code></li>
 * <li>edges: <code>-[id=42 :TYPE {prop1: ["a", 13]}]-</code></li>
 * <li>paths: <code>(id=1 :L)-[id=42 :T {k: "v"}]->(id=2)-...</code></li>
 * <li>points are serialized to geojson</li>
 * <li>maps: <code>{foo: 42, bar: "baz"}</code></li>
 * <li>lists and arrays: <code>["aa", "bb", "cc"]</code></li>
 * <li>Numbers: <code>2.7182818285</code></li>
 * <li>Strings: <code>"this is a string"</code></li>
 * </ul>
 */
public class PrettyPrinter implements AnyValueWriter<RuntimeException>
{
    private final Deque<Writer> stack = new ArrayDeque<>();
    private final String quoteMark;
    private final StringBuilder builder;
    private final EntityMode entityMode;

    public PrettyPrinter()
    {
        this( EntityMode.FULL );
    }

    public PrettyPrinter( EntityMode entityMode )
    {
        this( "\"", entityMode );
    }

    public PrettyPrinter( String quoteMark, EntityMode entityMode )
    {
        this.quoteMark = quoteMark;
        this.entityMode = entityMode;
        builder = new StringBuilder( 64 );
        stack.push( new ValueWriter( builder, quoteMark ) );
    }

    @Override
    public EntityMode entityMode()
    {
        return entityMode;
    }

    @Override
    public void writeNodeReference( long nodeId )
    {
        append( "(id=" ).append( String.valueOf( nodeId ) ).append( ")" );
    }

    @Override
    public void writeNode( long nodeId, TextArray labels, MapValue properties )
    {
        append( "(id=" ).append( String.valueOf( nodeId ) );
        String sep = " ";
        for ( int i = 0; i < labels.length(); i++ )
        {
            append( sep );
            append( ":" ).append( labels.stringValue( i ) );
            sep = "";
        }
        if ( properties.size() > 0 )
        {
            append( " " );
            properties.writeTo( this );
        }

        append( ")" );
    }

    @Override
    public void writeRelationshipReference( long relId )
    {
        append( "-[id=" ).append( String.valueOf( relId ) ).append( "]-" );
    }

    @Override
    public void writeRelationship( long relId, long startNodeId, long endNodeId, TextValue type, MapValue properties )
    {
        append( "-[id=" ).append( String.valueOf( relId ) ).append( " :" ).append( type.stringValue() );
        if ( properties.size() > 0 )
        {
            append( " " );
            properties.writeTo( this );
        }
        append( "]-" );
    }

    @Override
    public void beginMap( int size )
    {
        stack.push( new MapWriter( builder, quoteMark ) );
    }

    @Override
    public void endMap()
    {
        assert !stack.isEmpty();
        stack.pop().done();
        if ( !stack.isEmpty() )
        {
            stack.peek().next();
        }
    }

    @Override
    public void beginList( int size )
    {
        stack.push( new ListWriter( builder, quoteMark ) );
    }

    @Override
    public void endList()
    {
        assert !stack.isEmpty();
        stack.pop().done();
        if ( !stack.isEmpty() )
        {
            stack.peek().next();
        }
    }

    @Override
    public void writePath( NodeValue[] nodes, RelationshipValue[] relationships )
    {
        if ( nodes.length == 0 )
        {
            return;
        }
        //Path guarantees that nodes.length = edges.length = 1
        nodes[0].writeTo( this );
        for ( int i = 0; i < relationships.length; i++ )
        {
            relationships[i].writeTo( this );
            append( ">" );
            nodes[i + 1].writeTo( this );
        }

    }

    @Override
    public void writePoint( CoordinateReferenceSystem crs, double[] coordinate ) throws RuntimeException
    {
        append( "{geometry: {type: \"Point\", coordinates: " );
        append( Arrays.toString( coordinate ) );
        append( ", crs: {type: link, properties: {href: \"" );
        append( crs.getHref() );
        append( "\", code: " );
        append( Integer.toString( crs.getCode() ) );
        append( "}}}}" );
    }

    @Override
    public void writeDuration( long months, long days, long seconds, int nanos ) throws RuntimeException
    {
        append( "{duration: {months: " );
        append( Long.toString( months ) );
        append( ", days: " );
        append( Long.toString( days ) );
        append( ", seconds: " );
        append( Long.toString( seconds ) );
        append( ", nanos: " );
        append( Long.toString( nanos ) );
        append( "}}" );
    }

    @Override
    public void writeDate( LocalDate localDate ) throws RuntimeException
    {
        append( "{date: " );
        appendQuoted( localDate.toString() );
        append( "}" );
    }

    @Override
    public void writeLocalTime( LocalTime localTime ) throws RuntimeException
    {
        append( "{localTime: " );
        appendQuoted( localTime.toString() );
        append( "}" );
    }

    @Override
    public void writeTime( OffsetTime offsetTime ) throws RuntimeException
    {
        append( "{time: " );
        appendQuoted( offsetTime.toString() );
        append( "}" );
    }

    @Override
    public void writeLocalDateTime( LocalDateTime localDateTime ) throws RuntimeException
    {
        append( "{localDateTime: " );
        appendQuoted( localDateTime.toString() );
        append( "}" );
    }

    @Override
    public void writeDateTime( ZonedDateTime zonedDateTime ) throws RuntimeException
    {
        append( "{datetime: " );
        appendQuoted( zonedDateTime.toString() );
        append( "}" );
    }

    @Override
    public void writeNull()
    {
        append( "<null>" );
    }

    @Override
    public void writeBoolean( boolean value )
    {
        append( Boolean.toString( value ) );
    }

    @Override
    public void writeInteger( byte value )
    {
        append( Byte.toString( value ) );
    }

    @Override
    public void writeInteger( short value )
    {
        append( Short.toString( value ) );
    }

    @Override
    public void writeInteger( int value )
    {
        append( Integer.toString( value ) );
    }

    @Override
    public void writeInteger( long value )
    {
        append( Long.toString( value ) );
    }

    @Override
    public void writeFloatingPoint( float value )
    {
        append( Float.toString( value ) );
    }

    @Override
    public void writeFloatingPoint( double value )
    {
        append( Double.toString( value ) );
    }

    @Override
    public void writeString( String value )
    {
        appendQuoted( value );
    }

    @Override
    public void writeString( char value )
    {
        writeString( Character.toString( value ) );
    }

    @Override
    public void beginArray( int size, ArrayType arrayType )
    {
        stack.push( new ListWriter( builder, quoteMark ) );
    }

    @Override
    public void endArray()
    {
        assert !stack.isEmpty();
        stack.pop().done();
    }

    @Override
    public void writeByteArray( byte[] value )
    {
        String sep = "";
        append( "[" );
        for ( byte b : value )
        {
            append( sep );
            append( Byte.toString( b ) );
            sep = ", ";
        }
        append( "]" );
    }

    public String value()
    {
        assert stack.size() == 1;
        stack.getLast().done();
        return builder.toString();
    }

    public void valueInto( StringBuilder target )
    {
        assert stack.size() == 1;
        stack.getLast().done();
        target.append( builder );
    }

    private Writer append( String value )
    {
        assert !stack.isEmpty();
        Writer head = stack.peek();
        head.append( value );
        return head;
    }

    private void appendQuoted( String value )
    {
        assert !stack.isEmpty();
        Writer head = stack.peek();
        head.appendQuoted( value );
    }

    private interface Writer
    {
        Writer append( String value );

        void appendQuoted( String value );

        void next();

        void done();
    }

    private abstract static class BaseWriter implements Writer
    {
        protected final StringBuilder builder;
        private final String quoteMark;

        protected BaseWriter( StringBuilder builder, String quoteMark )
        {
            this.builder = builder;
            this.quoteMark = quoteMark;
        }

        @Override
        public void appendQuoted( String value )
        {
            builder.append( quoteMark );
            this.append( value );
            builder.append( quoteMark );
        }

        @Override
        public void next()
        {
        }

        @Override
        public void done()
        {
        }
    }

    private static class ValueWriter extends BaseWriter
    {
        private ValueWriter( StringBuilder builder, String quoteMark )
        {
            super( builder, quoteMark );
        }

        @Override
        public Writer append( String value )
        {
            builder.append( value );
            return this;
        }
    }

    private static class MapWriter extends BaseWriter
    {
        private boolean writeKey = true;
        private String sep = "";

        MapWriter( StringBuilder builder, String quoteMark )
        {
            super( builder, quoteMark );
            builder.append( '{' );
        }

        @Override
        public Writer append( String value )
        {
            if ( writeKey )
            {
                builder.append( sep ).append( value ).append( ": " );
            }
            else
            {
                builder.append( value );
            }
            writeKey = !writeKey;
            sep = ", ";
            return this;
        }

        @Override
        public void appendQuoted( String value )
        {
            if ( writeKey )
            {
                append( value );
            }
            else
            {
                super.appendQuoted( value );
            }
        }

        @Override
        public void next()
        {
            writeKey = true;
        }

        @Override
        public void done()
        {
            builder.append( '}' );
        }
    }

    private class ListWriter extends BaseWriter
    {
        private String sep = "";

        ListWriter( StringBuilder builder, String quoteMark )
        {
            super( builder, quoteMark );
            builder.append( '[' );
        }

        @Override
        public Writer append( String value )
        {
            builder.append( sep ).append( value );
            sep = ", ";
            return this;
        }

        @Override
        public void appendQuoted( String value )
        {
            builder.append( sep ).append( quoteMark ).append( value ).append( quoteMark );
            sep = ", ";
        }

        @Override
        public void done()
        {
            builder.append( ']' );
        }
    }
}
