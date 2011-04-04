package org.neo4j.com;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.jboss.netty.buffer.ChannelBuffer;
import org.neo4j.kernel.impl.nioneo.store.StoreId;

public class MadeUpClient extends Client<MadeUpCommunicationInterface> implements MadeUpCommunicationInterface
{
    private final StoreId storeIdToExpect;

    public MadeUpClient( int port, StoreId storeIdToExpect )
    {
        super( "localhost", port, new NotYetExistingGraphDatabase( "target/something" ) );
        this.storeIdToExpect = storeIdToExpect;
    }

    @Override
    public Response<Integer> multiply( final int value1, final int value2 )
    {
        return sendRequest( DumbRequestType.MULTIPLY, SlaveContext.EMPTY, new Serializer()
        {
            @Override
            public void write( ChannelBuffer buffer, ByteBuffer readBuffer ) throws IOException
            {
                buffer.writeInt( value1 );
                buffer.writeInt( value2 );
            }
        }, Protocol.INTEGER_DESERIALIZER );
    }

    @Override
    public Response<Void> streamSomeData( final MadeUpWriter writer, final int dataSize )
    {
        return sendRequest( DumbRequestType.STREAM_SOME_DATA, SlaveContext.EMPTY, new Serializer()
        {
            @Override
            public void write( ChannelBuffer buffer, ByteBuffer readBuffer ) throws IOException
            {
                buffer.writeInt( dataSize );
            }
        }, new Deserializer<Void>()
        {
            @Override
            public Void read( ChannelBuffer buffer, ByteBuffer temporaryBuffer )
                    throws IOException
            {
                writer.write( new BlockLogReader( buffer ) );
                return null;
            }
        } );
    }
    
    @Override
    protected StoreId getMyStoreId()
    {
        return storeIdToExpect;
    }
    
    static enum DumbRequestType implements RequestType<MadeUpCommunicationInterface>
    {
        MULTIPLY( new MasterCaller<MadeUpCommunicationInterface, Integer>()
        {
            @Override
            public Response<Integer> callMaster( MadeUpCommunicationInterface master,
                    SlaveContext context, ChannelBuffer input, ChannelBuffer target )
            {
                int value1 = input.readInt();
                int value2 = input.readInt();
                return master.multiply( value1, value2 );
            }
        }, Protocol.INTEGER_SERIALIZER ),
        
        STREAM_SOME_DATA( new MasterCaller<MadeUpCommunicationInterface, Void>()
        {
            @Override
            public Response<Void> callMaster( MadeUpCommunicationInterface master,
                    SlaveContext context, ChannelBuffer input, ChannelBuffer target )
            {
                int dataSize = input.readInt();
                return master.streamSomeData( new ToChannelBufferWriter( target ), dataSize );
            }
        }, Protocol.VOID_SERIALIZER );
        
        private final MasterCaller masterCaller;
        private final ObjectSerializer serializer;
        
        DumbRequestType( MasterCaller masterCaller, ObjectSerializer serializer )
        {
            this.masterCaller = masterCaller;
            this.serializer = serializer;
        }

        @Override
        public MasterCaller getMasterCaller()
        {
            return this.masterCaller;
        }

        @Override
        public ObjectSerializer getObjectSerializer()
        {
            return this.serializer;
        }

        @Override
        public byte id()
        {
            return (byte) ordinal();
        }
    }
}
