package org.neo4j.kernel.ha;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.embedder.DecoderEmbedder;
import org.jboss.netty.handler.codec.embedder.EncoderEmbedder;
import org.jboss.netty.handler.stream.ChunkedWriteHandler;
import org.junit.Test;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.ha.CommunicationProtocol.Deserializer;
import org.neo4j.kernel.ha.CommunicationProtocol.ObjectSerializer;

public class TestServerToClientCommunication
{
    @Test
    public void canWriteResponseWithoutTransactionStream() throws Exception
    {
        test( new Response<String>( "hello world", null ), STRING_DESERIALIZER );
    }

    @SuppressWarnings( "boxing" )
    @Test
    public void canWriteResponseWithSingleResourceSingleTransactionStream() throws Exception
    {
        TransactionStreams streams = new TransactionStreams();
        Collection<Pair<Long, ReadableByteChannel>> data = new ArrayList<Pair<Long, ReadableByteChannel>>();
        data.add( new Pair<Long, ReadableByteChannel>( 0x0123456789ABCDEFL, new ByteArrayChannel(
                (byte) 0x00, (byte) 0x99, (byte) 0xCC ) ) );
        TransactionStream stream = new TransactionStream( data );
        streams.add( "test resource", stream );
        test( new Response<String>( "hello world", streams ), STRING_DESERIALIZER );
    }

    @Test
    public void canWriteResponseWithSingleResourceNullTransactionStream() throws Exception
    {
        TransactionStreams streams = new TransactionStreams();
        Collection<Pair<Long, ReadableByteChannel>> data = new ArrayList<Pair<Long, ReadableByteChannel>>();
        TransactionStream stream = new TransactionStream( data );
        streams.add( "test resource", stream );
        test( new Response<String>( "hello world", streams ), STRING_DESERIALIZER );
    }

    @SuppressWarnings( "boxing" )
    @Test
    public void canWriteResponseWithSingleResourceMultiTransactionStream() throws Exception
    {
        TransactionStreams streams = new TransactionStreams();
        Collection<Pair<Long, ReadableByteChannel>> data = new ArrayList<Pair<Long, ReadableByteChannel>>();
        data.add( new Pair<Long, ReadableByteChannel>( 0x0123456789ABCDEFL, new ByteArrayChannel(
                (byte) 0x00, (byte) 0x99, (byte) 0xCC ) ) );
        data.add( new Pair<Long, ReadableByteChannel>( 0xFEDCBA9876543210L, new ByteArrayChannel(
                (byte) 0x00, (byte) 0x11, (byte) 0x22, (byte) 0x33, (byte) 0x44, (byte) 0x55,
                (byte) 0x66, (byte) 0x77, (byte) 0x88, (byte) 0x99, (byte) 0xAA, (byte) 0xBB,
                (byte) 0xCC, (byte) 0xDD, (byte) 0xEE, (byte) 0xFF ) ) );
        data.add( new Pair<Long, ReadableByteChannel>( 0xDEADCAFEBABEBEEFL, new ByteArrayChannel(
                (byte) 0xFF, (byte) 0xEE, (byte) 0xDD, (byte) 0xCC, (byte) 0xBB, (byte) 0xAA,
                (byte) 0x99, (byte) 0x88, (byte) 0x77, (byte) 0x66, (byte) 0x55, (byte) 0x44,
                (byte) 0x33, (byte) 0x22, (byte) 0x11, (byte) 0x00 ) ) );
        TransactionStream stream = new TransactionStream( data );
        streams.add( "test resource", stream );
        test( new Response<String>( "hello world", streams ), STRING_DESERIALIZER );
    }

    @SuppressWarnings( { "unchecked", "boxing" } )
    @Test
    public void canWriteResponseWithMultiResourceSingleTransactionStream() throws Exception
    {
        TransactionStreams streams = new TransactionStreams();
        streams.add( "resource one", new TransactionStream(
                Arrays.asList( new Pair<Long, ReadableByteChannel>( 0x0123456789ABCDEFL,
                        new ByteArrayChannel( (byte) 0x00, (byte) 0x99, (byte) 0xCC ) ) ) ) );
        streams.add( "resource two", new TransactionStream(
                Arrays.asList( new Pair<Long, ReadableByteChannel>( 0xFEDCBA9876543210L,
                        new ByteArrayChannel( (byte) 0x00, (byte) 0x11, (byte) 0x22, (byte) 0x33,
                                (byte) 0x44, (byte) 0x55, (byte) 0x66, (byte) 0x77, (byte) 0x88,
                                (byte) 0x99, (byte) 0xAA, (byte) 0xBB, (byte) 0xCC, (byte) 0xDD,
                                (byte) 0xEE, (byte) 0xFF ) ) ) ) );
        streams.add( "resource three", new TransactionStream(
                Arrays.asList( new Pair<Long, ReadableByteChannel>( 0xDEADCAFEBABEBEEFL,
                        new ByteArrayChannel( (byte) 0xFF, (byte) 0xEE, (byte) 0xDD, (byte) 0xCC,
                                (byte) 0xBB, (byte) 0xAA, (byte) 0x99, (byte) 0x88, (byte) 0x77,
                                (byte) 0x66, (byte) 0x55, (byte) 0x44, (byte) 0x33, (byte) 0x22,
                                (byte) 0x11, (byte) 0x00 ) ) ) ) );
        test( new Response<String>( "hello world", streams ), STRING_DESERIALIZER );
    }

    @SuppressWarnings( "boxing" )
    @Test
    public void canWriteResponseWithMultiResourceMultiTransactionStream() throws Exception
    {
        TransactionStreams streams = new TransactionStreams();
        // Stream 1
        Collection<Pair<Long, ReadableByteChannel>> data = new ArrayList<Pair<Long, ReadableByteChannel>>();
        data.add( new Pair<Long, ReadableByteChannel>( 0x0123456789ABCDEFL, new ByteArrayChannel(
                (byte) 0x00, (byte) 0x99, (byte) 0xCC ) ) );
        data.add( new Pair<Long, ReadableByteChannel>( 0xFEDCBA9876543210L, new ByteArrayChannel(
                (byte) 0x00, (byte) 0x11, (byte) 0x22, (byte) 0x33, (byte) 0x44, (byte) 0x55,
                (byte) 0x66, (byte) 0x77, (byte) 0x88, (byte) 0x99, (byte) 0xAA, (byte) 0xBB,
                (byte) 0xCC, (byte) 0xDD, (byte) 0xEE, (byte) 0xFF ) ) );
        data.add( new Pair<Long, ReadableByteChannel>( 0xDEADCAFEBABEBEEFL, new ByteArrayChannel(
                (byte) 0xFF, (byte) 0xEE, (byte) 0xDD, (byte) 0xCC, (byte) 0xBB, (byte) 0xAA,
                (byte) 0x99, (byte) 0x88, (byte) 0x77, (byte) 0x66, (byte) 0x55, (byte) 0x44,
                (byte) 0x33, (byte) 0x22, (byte) 0x11, (byte) 0x00 ) ) );
        streams.add( "test resource one", new TransactionStream( data ) );
        // Stream 2
        data = new ArrayList<Pair<Long, ReadableByteChannel>>();
        data.add( new Pair<Long, ReadableByteChannel>( 0x0123456789ABCDEFL, new ByteArrayChannel(
                (byte) 0x00, (byte) 0x99, (byte) 0xCC ) ) );
        data.add( new Pair<Long, ReadableByteChannel>( 0xFEDCBA9876543210L, new ByteArrayChannel(
                (byte) 0x00, (byte) 0xFF, (byte) 0x11, (byte) 0xEE, (byte) 0x22, (byte) 0xDD,
                (byte) 0x33, (byte) 0xCC, (byte) 0x44, (byte) 0xBB, (byte) 0x55, (byte) 0xAA,
                (byte) 0x66, (byte) 0x99, (byte) 0x77, (byte) 0x88 ) ) );
        data.add( new Pair<Long, ReadableByteChannel>( 0xDEADCAFEBABEBEEFL, new ByteArrayChannel(
                (byte) 0xCC, (byte) 0x99, (byte) 0x00 ) ) );
        streams.add( "test resource two", new TransactionStream( data ) );
        // Stream 3
        data = new ArrayList<Pair<Long, ReadableByteChannel>>();
        data.add( new Pair<Long, ReadableByteChannel>( 0x0000000000000012L, new ByteArrayChannel(
                (byte) 0xCC, (byte) 0xAA, (byte) 0xFF, (byte) 0xEE, (byte) 0xBB, (byte) 0xAA,
                (byte) 0xBB, (byte) 0xEE ) ) );
        data.add( new Pair<Long, ReadableByteChannel>( 0x0000000000000013L, new ByteArrayChannel(
                (byte) 0xDD, (byte) 0xEE, (byte) 0xAA, (byte) 0xDD, (byte) 0xBB, (byte) 0xEE,
                (byte) 0xEE, (byte) 0xFF ) ) );
        streams.add( "test resource one", new TransactionStream( data ) );
        data = new ArrayList<Pair<Long, ReadableByteChannel>>();
        streams.add( "test resource three", new TransactionStream( data ) );
        // Execute
        test( new Response<String>( "hello world", streams ), STRING_DESERIALIZER );
    }

    private static class ByteArrayChannel implements ReadableByteChannel
    {
        private final byte[] data;
        private int pos;

        ByteArrayChannel( byte... data )
        {
            this.data = data;
            this.pos = 0;
        }

        public int read( ByteBuffer dst ) throws IOException
        {
            if ( pos >= data.length ) return -1;
            int size = Math.min( data.length - pos, dst.limit() - dst.position() );
            dst.put( data, pos, size );
            pos += size;
            return size;
        }

        public void close() throws IOException
        {
            pos = -1;
        }

        public boolean isOpen()
        {
            return pos > 0;
        }

        public void reset()
        {
            pos = 0;
        }
    }

    // HELPERS

    private static abstract class Codec<T> implements ObjectSerializer<T>, Deserializer<T>
    {
    }

    private static final Codec<String> STRING_DESERIALIZER = new Codec<String>()
    {
        public void write( String responseObject, ChannelBuffer result )
        {
            result.writeShort( responseObject.length() );
            for ( char chr : responseObject.toCharArray() )
            {
                result.writeChar( chr );
            }
        }

        public String read( ChannelBuffer buffer )
        {
            char[] result = new char[buffer.readShort()];
            for ( int i = 0; i < result.length; i++ )
            {
                result[i] = buffer.readChar();
            }
            return new String( result );
        }
    };

    private <T> void test( Response<T> response, Codec<T> codec )
    {
        reader.setDeserializer( codec );
        assertEquality( response, roundtrip( response, codec ) );
    }

    private void assertEquality( Response<?> expected, Response<?> actual )
    {
        assertEquals( expected.response(), actual.response() );
        assertEquality( expected.transactions(), actual.transactions() );
    }

    @SuppressWarnings( { "null", "boxing" } )
    private void assertEquality( TransactionStreams expected, TransactionStreams actual )
    {
        if ( expected == null || expected.getStreams().isEmpty() )
        {
            assertTrue( "expected no transactions", actual == null || actual.getStreams().isEmpty() );
        }
        else
        {
            assertTrue( "expected transactions, but got none", actual != null
                                                               && !actual.getStreams().isEmpty() );
            Collection<Pair<String, TransactionStream>> expStreams = expected.getStreams();
            Collection<Pair<String, TransactionStream>> actStreams = actual.getStreams();
            assertEquals( String.format( "Not equal number of streams, expected <%s> got <%s>",
                    expStreams.size(), actStreams.size() ), expStreams.size(), actStreams.size() );
            Iterator<Pair<String, TransactionStream>> iterExp = expStreams.iterator();
            Iterator<Pair<String, TransactionStream>> iterAct = actStreams.iterator();
            while ( iterExp.hasNext() )
            {
                Pair<String, TransactionStream> exp = iterExp.next(), act = iterAct.next();
                assertEquals( exp.first(), act.first() );
                Collection<Pair<Long, ReadableByteChannel>> expData = exp.other().getChannels();
                Collection<Pair<Long, ReadableByteChannel>> actData = act.other().getChannels();
                assertEquals( String.format(
                        "Not equal number of transactions, expected <%s> got <%s>", expData.size(),
                        actData.size() ), expData.size(), actData.size() );
                if ( expData.isEmpty() ) continue;
                Iterator<Pair<Long, ReadableByteChannel>> expIter = expData.iterator();
                Iterator<Pair<Long, ReadableByteChannel>> actIter = actData.iterator();
                while ( expIter.hasNext() )
                {
                    Pair<Long, ReadableByteChannel> e = expIter.next(), a = actIter.next();
                    assertEquals( e.first(), a.first() );
                    assertEquality( String.format( "%s:0x%X", exp.first(), e.first() ),//
                            e.other(), a.other() );
                }
            }
        }
    }

    private void assertEquality( String txId, ReadableByteChannel expected,
            ReadableByteChannel actual )
    {
        ( (ByteArrayChannel) expected ).reset();
        byte[] exp = read( "expected data (txId = " + txId + ")", expected );
        byte[] act = read( "actual data (txId = " + txId + ")", actual );
        assertTrue( String.format( "Expected %s, got %s", Arrays.toString( exp ),
                Arrays.toString( act ) ), Arrays.equals( exp, act ) );
    }

    private byte[] read( String message, ReadableByteChannel data )
    {
        ByteBuffer buffer = ByteBuffer.allocate( 512 );
        try
        {
            int size = data.read( buffer );
            if ( size < 0 )
            {
                fail( "End of stream when reading " + message );
                return null;
            }
            byte[] result = new byte[size];
            buffer.flip();
            buffer.get( result );
            return result;
        }
        catch ( IOException e )
        {
            e.printStackTrace();
            fail( "IOException when reading stream while reading " + message );
            return null;
        }
    }

    @SuppressWarnings( "unchecked" )
    private <T> Response<T> roundtrip( Response<T> data, ObjectSerializer<T> serializer )
    {
        encoder.offer( new ChunkedResponse( data, serializer, data.transactions() != null ) );
        decoder.offer( encoder.poll() );
        return (Response<T>) decoder.poll();
    }

    private final InputReader reader = new InputReader();
    private final EncoderEmbedder<ChannelBuffer> encoder = new EncoderEmbedder<ChannelBuffer>(
            new ChunkedWriteHandler() );
    private final DecoderEmbedder<Response<?>> decoder = new DecoderEmbedder<Response<?>>( reader );
}
