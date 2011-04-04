/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
