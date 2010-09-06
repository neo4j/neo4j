package org.neo4j.kernel.ha;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.util.Collection;
import java.util.Iterator;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.stream.ChunkedInput;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.ha.CommunicationProtocol.ByteData;
import org.neo4j.kernel.ha.CommunicationProtocol.ObjectSerializer;

class ChunkedResponse implements ChunkedInput
{
    private final int CHUNK_SIZE = 2048;
    private final int CHOP_SIZE = 512;
    private Response<?> response;
    private ChannelBuffer chunk;
    private WriterState state;
    private final ObjectSerializer<Object> serializer;

    @SuppressWarnings( "unchecked" )
    ChunkedResponse( Response<?> response, ObjectSerializer serializer, boolean writeTransactions )
    {
        this.response = response;
        this.serializer = serializer;
        this.state = new WritePayload( writeTransactions ? new WriteTransactionStreamHeader()
                : WriterState.DONE );
    }

    private static abstract class WriterState
    {
        static final WriterState DONE = new WriterState()
        {
            @Override
            public ChannelBuffer write()
            {
                return null;
            }

            @Override
            public void write( ChannelBuffer buffer )
            {
                buffer.writeByte( 0 ); // Writes "no transaction streams"
            }
        };

        ChannelBuffer write()
        {
            ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
            write( buffer );
            return buffer;
        }

        abstract void write( ChannelBuffer buffer );
    }

    private class WritePayload extends WriterState
    {
        private final WriterState next;

        WritePayload( WriterState next )
        {
            this.next = next;
        }

        @Override
        void write( ChannelBuffer buffer )
        {
            int position = buffer.writerIndex();
            buffer.writeShort( 0 ); // Reserve space for a size header
            // Write the actual message
            serializer.write( response.response(), buffer );
            // Compute and write the size header
            buffer.setShort( position, buffer.writerIndex() - position - /*size of size header*/2 );
            ( state = next ).write( buffer );
        }
    }

    private class WriteTransactionStreamHeader extends WriterState
    {
        @Override
        void write( ChannelBuffer buffer )
        {
            Collection<Pair<String, TransactionStream>> streams = response.transactions().getStreams();
            buffer.writeByte( streams.size() ); // Number of transaction streams
            if ( streams.isEmpty() )
            {
                state = WriterState.DONE;
            }
            else
            {
                ( state = new WriteTransactionStreams( streams.iterator() ) ).write( buffer );
            }
        }
    }

    private class WriteTransactionStreams extends WriterState
    {
        private final Iterator<Pair<String, TransactionStream>> streams;
        private Pair<String, TransactionStream> current;

        public WriteTransactionStreams( Iterator<Pair<String, TransactionStream>> streams )
        {
            if ( streams.hasNext() )
            {
                this.current = streams.next();
            }
            else
            {
                this.current = null;
            }
            this.streams = streams;
        }

        @Override
        ChannelBuffer write()
        {
            if ( current == null ) return null;
            return super.write();
        }

        @Override
        void write( ChannelBuffer buffer )
        {
            for ( int space = CHUNK_SIZE - buffer.writerIndex(); current != null && space > 0; //
            /*      */space = CHUNK_SIZE - buffer.writerIndex() )
            {
                char[] resource = current.first().toCharArray();
                if ( space > /*|name|*/1 + resource.length + /*numTx*/1 )
                {
                    writeName( resource, buffer );
                }
                else
                {
                    return; // resource name will not fit under chunk size
                }
                Collection<Pair<Long, ReadableByteChannel>> channels = current.other().getChannels();
                buffer.writeByte( channels.size() ); // numTx
                ( state = new WriteStream( this, channels ) ).write( buffer );
                if ( streams.hasNext() )
                {
                    current = streams.next();
                }
                else
                {
                    current = null;
                }
            }
        }
    }

    static void writeName( char[] name, ChannelBuffer buffer )
    {
        buffer.writeByte( name.length );
        for ( char c : name )
        {
            buffer.writeChar( c );
        }
    }

    private class WriteStream extends WriterState
    {
        private final WriteTransactionStreams next;
        private final Iterator<Pair<Long/*txId*/, ReadableByteChannel>> stream;
        private Pair<Long/*txId*/, ByteData> current;
        private Iterator<byte[]> data;

        WriteStream( WriteTransactionStreams next,
                Collection<Pair<Long, ReadableByteChannel>> channels )
        {
            this.next = next;
            this.stream = channels.iterator();
            this.current = next();
        }

        private Pair<Long, ByteData> next()
        {
            this.data = null;
            if ( stream.hasNext() )
            {
                Pair<Long, ReadableByteChannel> source = stream.next();
                try
                {
                    return new Pair<Long, ByteData>( source.first(), new ByteData( source.other(),
                            CHOP_SIZE ) );
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( "failed to read byte channel", e );
                }
            }
            else
            {
                return null;
            }
        }

        @Override
        ChannelBuffer write()
        {
            ChannelBuffer buffer = super.write();
            if ( state != this /*we are done*/)
            {
                state.write( buffer );
            }
            return buffer;
        }

        @SuppressWarnings( "boxing" )
        @Override
        void write( ChannelBuffer buffer )
        {
            for ( int space = CHUNK_SIZE - buffer.writerIndex(); current != null && space > 0; //
            /*      */space = CHUNK_SIZE - buffer.writerIndex() )
            {
                if ( data == null )
                {
                    if ( space < 12 ) return;
                    buffer.writeLong( current.first() ); // Transaction ID
                    buffer.writeInt( current.other().size() ); // TX size
                    data = current.other().iterator();
                }
                else if ( data.hasNext() )
                {
                    if ( space < CHOP_SIZE ) return;
                    buffer.writeBytes( data.next() ); // Transaction data
                }
                else
                {
                    data = null;
                    current = next();
                }
            }
            if ( current == null )
            {
                state = next;
            }
        }
    }

    public boolean hasNextChunk()
    {
        if ( chunk != null ) return true;
        if ( response == null ) return false;
        chunk = state.write();
        if ( chunk == null || !chunk.readable() )
        { // No more data!
            chunk = null;
            close(); // we are done!
            return false;
        }
        else
        {
            return true;
        }
    }

    public final ChannelBuffer nextChunk()
    {
        if ( hasNextChunk() )
        {
            try
            {
                return chunk;
            }
            finally
            {
                chunk = null;
            }
        }
        else
        {
            return null;
        }
    }

    public final void close()
    {
        response = null;
        state = WriterState.DONE;
    }

    public final boolean isEndOfInput()
    {
        return !hasNextChunk();
    }
}
