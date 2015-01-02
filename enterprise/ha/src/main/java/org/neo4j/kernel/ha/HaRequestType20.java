/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.ha;

import static org.neo4j.com.Protocol.INTEGER_SERIALIZER;
import static org.neo4j.com.Protocol.LONG_SERIALIZER;
import static org.neo4j.com.Protocol.VOID_SERIALIZER;
import static org.neo4j.com.Protocol.readBoolean;
import static org.neo4j.com.Protocol.readString;
import static org.neo4j.kernel.ha.com.slave.MasterClient.LOCK_SERIALIZER;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

import org.jboss.netty.buffer.ChannelBuffer;
import org.neo4j.com.BlockLogReader;
import org.neo4j.com.ObjectSerializer;
import org.neo4j.com.RequestContext;
import org.neo4j.com.RequestType;
import org.neo4j.com.Response;
import org.neo4j.com.TargetCaller;
import org.neo4j.com.storecopy.ToNetworkStoreWriter;
import org.neo4j.com.TxExtractor;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.ha.com.master.HandshakeResult;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.com.slave.MasterClient18.AquireLockCall;
import org.neo4j.kernel.ha.id.IdAllocation;
import org.neo4j.kernel.ha.lock.LockResult;
import org.neo4j.kernel.impl.nioneo.store.IdRange;
import org.neo4j.kernel.monitoring.Monitors;

public enum HaRequestType20 implements RequestType<Master>
{
    // ====
    ALLOCATE_IDS( new TargetCaller<Master, IdAllocation>()
    {
        @Override
        public Response<IdAllocation> call( Master master, RequestContext context, ChannelBuffer input,
                ChannelBuffer target )
        {
            IdType idType = IdType.values()[input.readByte()];
            return master.allocateIds( context, idType );
        }
    }, new ObjectSerializer<IdAllocation>()
    {
        @Override
        public void write( IdAllocation idAllocation, ChannelBuffer result ) throws IOException
        {
            IdRange idRange = idAllocation.getIdRange();
            result.writeInt( idRange.getDefragIds().length );
            for ( long id : idRange.getDefragIds() )
            {
                result.writeLong( id );
            }
            result.writeLong( idRange.getRangeStart() );
            result.writeInt( idRange.getRangeLength() );
            result.writeLong( idAllocation.getHighestIdInUse() );
            result.writeLong( idAllocation.getDefragCount() );
        }
    }
    ),

    // ====
    CREATE_RELATIONSHIP_TYPE( new TargetCaller<Master, Integer>()
    {
        @Override
        public Response<Integer> call( Master master, RequestContext context, ChannelBuffer input,
                ChannelBuffer target )
        {
            return master.createRelationshipType( context, readString( input ) );
        }
    }, INTEGER_SERIALIZER ),

    // ====
    ACQUIRE_NODE_WRITE_LOCK( new AquireLockCall()
    {
        @Override
        public Response<LockResult> lock( Master master, RequestContext context, long... ids )
        {
            return master.acquireNodeWriteLock( context, ids );
        }
    }, LOCK_SERIALIZER )
    {
        @Override
        public boolean isLock()
        {
            return true;
        }
    },

    // ====
    ACQUIRE_NODE_READ_LOCK( new AquireLockCall()
    {
        @Override
        public Response<LockResult> lock( Master master, RequestContext context, long... ids )
        {
            return master.acquireNodeReadLock( context, ids );
        }
    }, LOCK_SERIALIZER )
    {
        @Override
        public boolean isLock()
        {
            return true;
        }
    },

    // ====
    ACQUIRE_RELATIONSHIP_WRITE_LOCK( new AquireLockCall()
    {
        @Override
        public Response<LockResult> lock( Master master, RequestContext context, long... ids )
        {
            return master.acquireRelationshipWriteLock( context, ids );
        }
    }, LOCK_SERIALIZER )
    {
        @Override
        public boolean isLock()
        {
            return true;
        }
    },

    // ====
    ACQUIRE_RELATIONSHIP_READ_LOCK( new AquireLockCall()
    {
        @Override
        public Response<LockResult> lock( Master master, RequestContext context, long... ids )
        {
            return master.acquireRelationshipReadLock( context, ids );
        }
    }, LOCK_SERIALIZER )
    {
        @Override
        public boolean isLock()
        {
            return true;
        }
    },

    // ====
    COMMIT( new TargetCaller<Master, Long>()
    {
        @Override
        public Response<Long> call( Master master, RequestContext context, ChannelBuffer input,
                ChannelBuffer target )
        {
            String resource = readString( input );
            final ReadableByteChannel reader = new BlockLogReader( input );
            return master.commitSingleResourceTransaction( context, resource, TxExtractor.create( reader ) );
        }
    }, LONG_SERIALIZER ),

    // ====
    PULL_UPDATES( new TargetCaller<Master, Void>()
    {
        @Override
        public Response<Void> call( Master master, RequestContext context, ChannelBuffer input,
                ChannelBuffer target )
        {
            return master.pullUpdates( context );
        }
    }, VOID_SERIALIZER ),

    // ====
    FINISH( new TargetCaller<Master, Void>()
    {
        @Override
        public Response<Void> call( Master master, RequestContext context, ChannelBuffer input,
                ChannelBuffer target )
        {
            return master.finishTransaction( context, readBoolean( input ) );
        }
    }, VOID_SERIALIZER ),

    // ====
    HANDSHAKE( new TargetCaller<Master, HandshakeResult>()
    {
        @Override
        public Response<HandshakeResult> call( Master master, RequestContext context, ChannelBuffer input,
                ChannelBuffer target )
        {
            return master.handshake( input.readLong(), null );
        }
    }, new ObjectSerializer<HandshakeResult>()
    {
        @Override
        public void write( HandshakeResult responseObject, ChannelBuffer result ) throws IOException
        {
            result.writeInt( responseObject.txAuthor() );
            result.writeLong( responseObject.txChecksum() );
        }
    } ),

    // ====
    COPY_STORE( new TargetCaller<Master, Void>()
    {
        @Override
        public Response<Void> call( Master master, RequestContext context, ChannelBuffer input,
                final ChannelBuffer target )
        {
            return master.copyStore( context, new ToNetworkStoreWriter( target, new Monitors() ) );
        }

    }, VOID_SERIALIZER ),

    // ====
    COPY_TRANSACTIONS( new TargetCaller<Master, Void>()
    {
        @Override
        public Response<Void> call( Master master, RequestContext context, ChannelBuffer input,
                final ChannelBuffer target )
        {
            return master.copyTransactions( context, readString( input ), input.readLong(), input.readLong() );
        }

    }, VOID_SERIALIZER ),

    // ====
    INITIALIZE_TX( new TargetCaller<Master, Void>()
    {
        @Override
        public Response<Void> call( Master master, RequestContext context, ChannelBuffer input,
                ChannelBuffer target )
        {
            return master.initializeTx( context );
        }
    }, VOID_SERIALIZER ),

    // ====
    ACQUIRE_GRAPH_WRITE_LOCK( new TargetCaller<Master, LockResult>()
    {
        @Override
        public Response<LockResult> call( Master master, RequestContext context, ChannelBuffer input,
                ChannelBuffer target )
        {
            return master.acquireGraphWriteLock( context );
        }
    }, LOCK_SERIALIZER )
    {
        @Override
        public boolean isLock()
        {
            return true;
        }
    },

    // ====
    ACQUIRE_GRAPH_READ_LOCK( new TargetCaller<Master, LockResult>()
    {
        @Override
        public Response<LockResult> call( Master master, RequestContext context, ChannelBuffer input,
                ChannelBuffer target )
        {
            return master.acquireGraphReadLock( context );
        }
    }, LOCK_SERIALIZER )
    {
        @Override
        public boolean isLock()
        {
            return true;
        }
    },

    // ====
    ACQUIRE_INDEX_READ_LOCK( new TargetCaller<Master, LockResult>()
    {
        @Override
        public Response<LockResult> call( Master master, RequestContext context, ChannelBuffer input,
                ChannelBuffer target )
        {
            return master.acquireIndexReadLock( context, readString( input ), readString( input ) );
        }

    }, LOCK_SERIALIZER )
    {
        @Override
        public boolean isLock()
        {
            return true;
        }
    },

    // ====
    ACQUIRE_INDEX_WRITE_LOCK( new TargetCaller<Master, LockResult>()
    {
        @Override
        public Response<LockResult> call( Master master, RequestContext context, ChannelBuffer input,
                ChannelBuffer target )
        {
            return master.acquireIndexWriteLock( context, readString( input ), readString( input ) );
        }

    }, LOCK_SERIALIZER )
    {
        @Override
        public boolean isLock()
        {
            return true;
        }
    },

    // ====
    PUSH_TRANSACTION( new TargetCaller<Master, Void>()
    {
        @Override
        public Response<Void> call( Master master, RequestContext context, ChannelBuffer input,
                ChannelBuffer target )
        {
            return master.pushTransaction( context, readString( input ), input.readLong() );
        }
    }, VOID_SERIALIZER ),
    
    // ====
    CREATE_PROPERTY_KEY( new TargetCaller<Master, Integer>()
    {
        @Override
        public Response<Integer> call( Master master, RequestContext context, ChannelBuffer input,
                ChannelBuffer target )
        {
            return master.createPropertyKey( context, readString( input ) );
        }
    }, INTEGER_SERIALIZER ),

    // ====
    CREATE_LABEL( new TargetCaller<Master, Integer>()
    {
        @Override
        public Response<Integer> call( Master master, RequestContext context, ChannelBuffer input,
                                       ChannelBuffer target )
        {
            return master.createLabel( context, readString( input ) );
        }
    }, INTEGER_SERIALIZER ),

    // ====
    ACQUIRE_SCHEMA_READ_LOCK( new TargetCaller<Master, LockResult>()
    {
        @Override
        public Response<LockResult> call( Master master, RequestContext context, ChannelBuffer input,
                ChannelBuffer target )
        {
            return master.acquireSchemaReadLock( context );
        }

    }, LOCK_SERIALIZER )
    {
        @Override
        public boolean isLock()
        {
            return true;
        }
    },

    // ====
    ACQUIRE_SCHEMA_WRITE_LOCK( new TargetCaller<Master, LockResult>()
    {
        @Override
        public Response<LockResult> call( Master master, RequestContext context, ChannelBuffer input,
                ChannelBuffer target )
        {
            return master.acquireSchemaWriteLock( context );
        }

    }, LOCK_SERIALIZER )
    {
        @Override
        public boolean isLock()
        {
            return true;
        }
    }, ACQUIRE_INDEX_ENTRY_WRITE_LOCK( new TargetCaller<Master,LockResult>()
    {
        @Override
        public Response<LockResult> call( Master master, RequestContext context, ChannelBuffer input,
                                          ChannelBuffer target )
        {
            return master.acquireIndexEntryWriteLock( context, input.readLong(), input.readLong(), readString( input ) );
        }
    }, LOCK_SERIALIZER );


    @SuppressWarnings( "rawtypes" )
    final TargetCaller caller;
    @SuppressWarnings( "rawtypes" )
    final ObjectSerializer serializer;

    private <T> HaRequestType20( TargetCaller caller, ObjectSerializer<T> serializer )
    {
        this.caller = caller;
        this.serializer = serializer;
    }

    @Override
    public ObjectSerializer getObjectSerializer()
    {
        return serializer;
    }

    @Override
    public TargetCaller getTargetCaller()
    {
        return caller;
    }

    @Override
    public byte id()
    {
        return (byte) ordinal();
    }

    public boolean isLock()
    {
        return false;
    }
}