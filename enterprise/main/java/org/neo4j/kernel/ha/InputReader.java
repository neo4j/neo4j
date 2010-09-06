package org.neo4j.kernel.ha;

import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.UpstreamMessageEvent;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.ha.CommunicationProtocol.Deserializer;

public class InputReader extends SimpleChannelUpstreamHandler
{
    private volatile Deserializer<?> deserializer;
    private ReaderState state;

    public void setDeserializer( Deserializer<?> deserializer )
    {
        this.deserializer = deserializer;
    }

    private abstract class ReaderState
    {
        abstract Response<?> read( ChannelBuffer buffer );
    }

    abstract class ReadInteger extends ReaderState
    {
        private int bytesToRead;
        private byte[] bytes = null;

        ReadInteger( int size )
        {
            bytesToRead = size;
        }

        @Override
        Response<?> read( ChannelBuffer buffer )
        {
            if ( buffer.readableBytes() < bytesToRead || bytes != null )
            {
                if ( bytes == null )
                {
                    bytes = new byte[bytesToRead];
                }
                int count = 0;
                for ( int i = bytes.length - bytesToRead; i < bytes.length && buffer.readable(); i++, count++ )
                {
                    bytes[i] = buffer.readByte();
                }
                bytesToRead -= count;
                if ( bytesToRead <= 0 )
                {
                    long integer = 0;
                    if ( ByteOrder.LITTLE_ENDIAN.equals( buffer.order() ) )
                    {
                        for ( int i = bytes.length; i > 0; --i )
                        {
                            integer <<= 8;
                            integer |= bytes[i - 1] & 0xFF;
                        }
                    }
                    else
                    {
                        for ( int i = 0; i < bytes.length; i++ )
                        {
                            integer <<= 8;
                            integer |= bytes[i] & 0xFF;
                        }
                    }
                    done( integer );
                }
            }
            else
            {
                switch ( bytesToRead )
                {
                case 1:
                    state = done( buffer.readUnsignedByte() );
                    break;
                case 2:
                    state = done( buffer.readUnsignedShort() );
                    break;
                case 3:
                    state = done( buffer.readUnsignedMedium() );
                    break;
                case 4:
                    state = done( buffer.readUnsignedInt() );
                    break;
                case 8:
                    state = done( buffer.readLong() );
                    break;
                }
            }
            return null;
        }

        abstract ReaderState done( long value );
    }

    @Override
    public void messageReceived( ChannelHandlerContext ctx, MessageEvent e ) throws Exception
    {
        ChannelBuffer buffer = (ChannelBuffer) e.getMessage();
        while ( buffer.readable() )
        {
            if ( state == null )
            {
                state = new ReadInteger( 2 )
                {
                    @Override
                    ReaderState done( long value )
                    {
                        return new ReadMessage( (int) value );
                    }
                };
            }
            else
            {
                Response<?> response = state.read( buffer );
                if ( response != null )
                {
                    state = null;
                    ctx.sendUpstream( new UpstreamMessageEvent( e.getChannel(), response,
                            e.getRemoteAddress() ) );
                }
            }
        }
    }

    private class ReadMessage extends ReaderState
    {
        private int bytesLeft;
        private final Deserializer<?> format;
        private ChannelBuffer read = null;

        ReadMessage( int length )
        {
            this.bytesLeft = length;
            this.format = deserializer;
        }

        @Override
        Response<?> read( ChannelBuffer buffer )
        {
            if ( read == null )
            {
                if ( buffer.readableBytes() >= bytesLeft )
                {
                    read = buffer;
                    bytesLeft = 0;
                }
                else
                {
                    read = ChannelBuffers.buffer( bytesLeft );
                }
            }
            if ( bytesLeft > 0 )
            {
                int toRead = Math.min( buffer.readableBytes(), bytesLeft );
                buffer.readBytes( read, toRead );
                bytesLeft -= toRead;
            }
            if ( bytesLeft <= 0 )
            {
                state = new ReadStreamsHeader( format.read( read ) );
            }
            return null;
        }
    }

    private class ReadStreamsHeader extends ReaderState
    {
        private final Object payload;

        public ReadStreamsHeader( Object payload )
        {
            this.payload = payload;
        }

        @Override
        Response<?> read( ChannelBuffer buffer )
        {
            short numStreams = buffer.readUnsignedByte();
            if ( numStreams == 0 ) return new Response<Object>( payload, null );
            state = new ReadStreams( numStreams, payload );
            return null;
        }
    }

    private class ReadStreams extends ReaderState
    {
        private final Object payload;
        private int streamsLeft;
        private final TransactionStreams streams = new TransactionStreams();

        public ReadStreams( int numStreams, Object payload )
        {
            this.streamsLeft = numStreams;
            this.payload = payload;
        }

        @Override
        Response<?> read( ChannelBuffer buffer )
        {
            if ( streamsLeft == 0 ) return new Response<Object>( payload, streams );
            /* Data format, for each stream:
             *        1 b: |name|            - read by this (ReadStreams)
             * |name|*2 b: name              - read by ReadStreamName
             *        1 b: numTx             - read by ReadStreamName/ReadInteger(1)
             *        8 b: txId   \          - read by ReadSingleTransactionStream
             *        4 b: |tx|    } * numTx - read by ReadSingleTransactionStream/ReadInteger(4)
             *     |tx| b: txData /          - read by ReadTransactionData
             */
            state = new ReadStreamName( this, buffer.readUnsignedByte() );
            return null;
        }

        ReaderState addStream( String resource, TransactionStream stream )
        {
            streams.add( resource, stream );
            if ( --streamsLeft <= 0 )
            {
                return null;
            }
            else
            {
                return this;
            }
        }
    }

    private class ReadStreamName extends ReaderState
    {
        private final ReadStreams streams;
        private int bytesLeft;
        private final byte[] data;

        ReadStreamName( ReadStreams streams, short length )
        {
            this.streams = streams;
            this.bytesLeft = length * 2;
            this.data = new byte[bytesLeft];
        }

        @Override
        Response<?> read( ChannelBuffer buffer )
        {
            int toRead = Math.min( bytesLeft, buffer.readableBytes() );
            buffer.readBytes( data, data.length - bytesLeft, toRead );
            bytesLeft -= toRead;
            if ( bytesLeft <= 0 )
            {
                final String name = makeString( data, buffer.order() );
                state = new ReadInteger( 1 )
                {
                    @Override
                    ReaderState done( long numTx )
                    {
                        if ( numTx == 0 )
                        {
                            return streams.addStream( name, new TransactionStream(
                                    Collections.<Pair<Long, ReadableByteChannel>>emptyList() ) );
                        }
                        return new ReadSingleTransactionStream( streams, name, (int) numTx );
                    }

                    @Override
                    Response<?> read( ChannelBuffer buffer )
                    {
                        super.read( buffer );
                        if ( state == null )
                        {
                            return new Response<Object>( streams.payload, streams.streams );
                        }
                        else
                        {
                            return null;
                        }
                    }
                };
            } // else stay in this state
            return null;
        }
    }

    static String makeString( byte[] data, ByteOrder byteOrder )
    {
        char[] chars = new char[data.length / 2];
        if ( ByteOrder.LITTLE_ENDIAN.equals( byteOrder ) )
        {
            for ( int i = 0; i < chars.length; i++ )
            {
                chars[i] = (char) ( ( ( (int) data[2 * i + 1] ) << 8 ) | ( (int) data[2 * i] ) );
            }
        }
        else
        {
            for ( int i = 0; i < chars.length; i++ )
            {
                chars[i] = (char) ( ( ( (int) data[2 * i] ) << 8 ) | ( (int) data[2 * i + 1] ) );
            }
        }
        return new String( chars );
    }

    private class ReadSingleTransactionStream extends ReadInteger
    {
        private final ReadStreams streams;
        private final String resource;
        private final int transactionsLeft;
        private final Collection<Pair<Long, ReadableByteChannel>> data;

        ReadSingleTransactionStream( ReadStreams streams, String resource, int numTx )
        {
            this( streams, new ArrayList<Pair<Long, ReadableByteChannel>>( numTx ), resource, numTx );
        }

        public ReadSingleTransactionStream( ReadStreams streams,
                Collection<Pair<Long, ReadableByteChannel>> data, String resource,
                int transactionsLeft )
        {
            super( 8 );
            this.streams = streams;
            this.resource = resource;
            this.transactionsLeft = transactionsLeft;
            this.data = data;
        }

        @Override
        public ReaderState done( final long txId )
        {
            return new ReadInteger( 4 )
            {
                @Override
                ReaderState done( long size )
                {
                    return new ReadTransactionData( ReadSingleTransactionStream.this, txId,
                            (int) size );
                }
            };
        }

        @SuppressWarnings( "boxing" )
        ReaderState txDone( long txId, ChannelBuffer buffer )
        {
            data.add( new Pair<Long, ReadableByteChannel>( txId, new ByteReadableChannelBuffer( buffer ) ) );
            if ( transactionsLeft <= 1 )
            {
                return streams.addStream( resource, new TransactionStream( data ) );
            }
            else
            {
                return new ReadSingleTransactionStream( streams, data, resource,
                        transactionsLeft - 1 );
            }
        }

        public Response<?> product()
        {
            return new Response<Object>( streams.payload, streams.streams );
        }
    }

    private class ReadTransactionData extends ReaderState
    {
        private final ReadSingleTransactionStream stream;
        private final long txId;
        private int bytesLeft;
        private final ChannelBuffer data;

        ReadTransactionData( ReadSingleTransactionStream stream, long txId, int size )
        {
            this.stream = stream;
            this.txId = txId;
            this.bytesLeft = size;
            this.data = ChannelBuffers.buffer( size );
        }

        @Override
        public Response<?> read( ChannelBuffer buffer )
        {
            int toRead = Math.min( bytesLeft, buffer.readableBytes() );
            buffer.readBytes( data, toRead );
            bytesLeft -= toRead;
            if ( bytesLeft <= 0 )
            {
                state = stream.txDone( txId, data );
                if ( state == null )
                {
                    return stream.product();
                }
            }
            return null;
        }
    }
}
