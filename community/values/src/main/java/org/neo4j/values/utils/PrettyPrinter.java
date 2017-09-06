package org.neo4j.values.utils;

import java.util.ArrayDeque;
import java.util.Deque;

import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.CoordinateReferenceSystem;
import org.neo4j.values.virtual.EdgeValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;

import static java.lang.String.format;

/**
 * Pretty printer for AnyValues.
 *
 * Used to nicely format any given AnyValue.
 */
public class PrettyPrinter implements AnyValueWriter<RuntimeException>
{
    private final Deque<Writer> stack = new ArrayDeque<>();

    PrettyPrinter()
    {
        stack.push( new ValueWriter() );
    }

    interface Writer
    {
        void write( String value );

        String done();

        default String quote( String in )
        {
            return "'" + in + "'";
        }
    }

    static class ValueWriter implements Writer
    {
        private final StringBuilder builder = new StringBuilder();

        @Override
        public void write( String value )
        {
            builder.append( value );
        }

        @Override
        public String done()
        {
            return builder.toString();
        }
    }

    static class MapWriter implements Writer
    {
        private boolean writeKey = true;
        private String sep = "";
        private final StringBuilder builder;

        MapWriter()
        {
            this.builder = new StringBuilder( "{" );
        }

        @Override
        public void write( String value )
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
        }

        @Override
        public String done()
        {
            return builder.append( "}" ).toString();
        }

        @Override
        public String quote( String in )
        {
            return writeKey ? in : "'" + in + "'";
        }
    }

    static class ListWriter implements Writer
    {
        private final StringBuilder builder;
        private String sep = "";

        ListWriter()
        {
            this.builder = new StringBuilder( "[" );
        }

        @Override
        public void write( String value )
        {
            builder.append( sep ).append( value );
            sep = ", ";
        }

        @Override
        public String done()
        {
            return builder.append( "]" ).toString();
        }
    }

    static class PointWriter implements Writer
    {
        private int i = 0;
        private String[] coordinates = new String[2];
        private final CoordinateReferenceSystem crs;

        PointWriter( CoordinateReferenceSystem crs )
        {
            this.crs = crs;
        }

        @Override
        public void write( String value )
        {
            coordinates[i++] = value;
        }

        @Override
        public String done()
        {
            return format(
                    "{geometry: " +
                    "{type: 'Point', coordinates: [%s, %s], crs: " +
                    "{type: link, " +
                    "properties: {href: '%s', code: %d}" +
                    "}" +
                    "}" +
                    "}",
                    coordinates[0], coordinates[1], crs.href(), crs.code() );
        }
    }

    private void append( String value )
    {
        assert !stack.isEmpty();
        Writer head = stack.peek();
        head.write( value );
    }

    private String quote( String value )
    {
        assert !stack.isEmpty();
        Writer head = stack.peek();
        return head.quote( value );
    }

    @Override
    public void writeNodeReference( long nodeId )
    {
        append( format( "(id=%d)", nodeId ) );
    }

    @Override
    public void writeNode( long nodeId, TextArray labels, MapValue properties )
    {
        append( format( "(id=%d", nodeId ) );
        String sep = " ";
        for ( int i = 0; i < labels.length(); i++ )
        {
            append( sep );
            append( ":" + labels.stringValue( i ) );
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
    public void writeEdgeReference( long edgeId )
    {
        append( format( "-[id=%d]-", edgeId ) );
    }

    @Override
    public void writeEdge( long edgeId, long startNodeId, long endNodeId, TextValue type, MapValue properties )
    {
        append( format( "-[id=%d :%s", edgeId, type.stringValue() ) );
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
        stack.push( new MapWriter() );
    }

    @Override
    public void endMap()
    {
        assert !stack.isEmpty();
        append( stack.pop().done() );
    }

    @Override
    public void beginList( int size )
    {
        stack.push( new ListWriter() );
    }

    @Override
    public void endList()
    {
        assert !stack.isEmpty();
        append( stack.pop().done() );
    }

    @Override
    public void writePath( NodeValue[] nodes, EdgeValue[] edges )
    {
        if ( nodes.length == 0 )
        {
            return;
        }
        //Path guarantees that nodes.length = edges.length = 1
        nodes[0].writeTo( this );
        for ( int i = 0; i < edges.length; i++ )
        {
            edges[i].writeTo( this );
            append( ">" );
            nodes[i + 1].writeTo( this );
        }

    }

    @Override
    public void beginPoint( CoordinateReferenceSystem coordinateReferenceSystem )
    {
        stack.push( new PointWriter( coordinateReferenceSystem ) );
    }

    @Override
    public void endPoint()
    {
        assert !stack.isEmpty();
        append( stack.pop().done() );
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
        append( quote( value ) );
    }

    @Override
    public void writeString( char value )
    {
        writeString( Character.toString( value ) );
    }

    @Override
    public void writeString( char[] value, int offset, int length )
    {
        writeString( new String( value, offset, length ) );
    }

    @Override
    public void beginArray( int size, ArrayType arrayType )
    {
        stack.push( new ListWriter() );
    }

    @Override
    public void endArray()
    {
        assert !stack.isEmpty();
        append( stack.pop().done() );
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
        return stack.getLast().done();
    }
}
