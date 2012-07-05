/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
import static org.neo4j.kernel.ha.MasterClient.LOCK_SERIALIZER;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

import org.jboss.netty.buffer.ChannelBuffer;
import org.neo4j.com.BlockLogReader;
import org.neo4j.com.ObjectSerializer;
import org.neo4j.com.RequestContext;
import org.neo4j.com.RequestType;
import org.neo4j.com.Response;
import org.neo4j.com.TargetCaller;
import org.neo4j.com.ToNetworkStoreWriter;
import org.neo4j.com.TxExtractor;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.ha.MasterClient18.AquireLockCall;
import org.neo4j.kernel.impl.nioneo.store.IdRange;

public enum HaRequestType17 implements RequestType<Master>
{
    // ====
    ALLOCATE_IDS( new TargetCaller<Master, IdAllocation>()
    {
        @Override
        public Response<IdAllocation> call( Master master, RequestContext context, ChannelBuffer input,
                ChannelBuffer target )
        {
            IdType idType = IdType.values()[input.readByte()];
            return master.allocateIds( idType );
        }
    }, new ObjectSerializer<IdAllocation>()
    {
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
    }, false ),

    // ====
    CREATE_RELATIONSHIP_TYPE( new TargetCaller<Master, Integer>()
    {
        @Override
        public Response<Integer> call( Master master, RequestContext context, ChannelBuffer input,
                ChannelBuffer target )
        {
            return master.createRelationshipType( context, readString( input ) );
        }
    }, INTEGER_SERIALIZER, true ),

    // ====
    ACQUIRE_NODE_WRITE_LOCK( new AquireLockCall()
    {
        @Override
        Response<LockResult> lock( Master master, RequestContext context, long... ids )
        {
            return master.acquireNodeWriteLock( context, ids );
        }
    }, LOCK_SERIALIZER, true )
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
        Response<LockResult> lock( Master master, RequestContext context, long... ids )
        {
            return master.acquireNodeReadLock( context, ids );
        }
    }, LOCK_SERIALIZER, true )
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
        Response<LockResult> lock( Master master, RequestContext context, long... ids )
        {
            return master.acquireRelationshipWriteLock( context, ids );
        }
    }, LOCK_SERIALIZER, true )
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
        Response<LockResult> lock( Master master, RequestContext context, long... ids )
        {
            return master.acquireRelationshipReadLock( context, ids );
        }
    }, LOCK_SERIALIZER, true )
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
    }, LONG_SERIALIZER, true ),

    // ====
    PULL_UPDATES( new TargetCaller<Master, Void>()
    {
        @Override
        public Response<Void> call( Master master, RequestContext context, ChannelBuffer input,
                ChannelBuffer target )
        {
            return master.pullUpdates( context );
        }
    }, VOID_SERIALIZER, true ),

    // ====
    FINISH( new TargetCaller<Master, Void>()
    {
        @Override
        public Response<Void> call( Master master, RequestContext context, ChannelBuffer input,
                ChannelBuffer target )
        {
            return master.finishTransaction( context, readBoolean( input ) );
        }
    }, VOID_SERIALIZER, true ),

    // ====
    GET_MASTER_ID_FOR_TX( new TargetCaller<Master, Pair<Integer, Long>>()
    {
        @Override
        public Response<Pair<Integer, Long>> call( Master master, RequestContext context, ChannelBuffer input,
                ChannelBuffer target )
        {
            return master.getMasterIdForCommittedTx( input.readLong(), null );
        }
    }, new ObjectSerializer<Pair<Integer, Long>>()
    {
        @Override
        public void write( Pair<Integer, Long> responseObject, ChannelBuffer result ) throws IOException
        {
            result.writeInt( responseObject.first() );
            result.writeLong( responseObject.other() );
        }
    }, false ),

    // ====
    COPY_STORE( new TargetCaller<Master, Void>()
    {
        @Override
        public Response<Void> call( Master master, RequestContext context, ChannelBuffer input,
                final ChannelBuffer target )
        {
            return master.copyStore( context, new ToNetworkStoreWriter( target ) );
        }

    }, VOID_SERIALIZER, true ),

    // ====
    COPY_TRANSACTIONS( new TargetCaller<Master, Void>()
    {
        @Override
        public Response<Void> call( Master master, RequestContext context, ChannelBuffer input,
                final ChannelBuffer target )
        {
            return master.copyTransactions( context, readString( input ), input.readLong(), input.readLong() );
        }

    }, VOID_SERIALIZER, true ),

    // ====
    INITIALIZE_TX( new TargetCaller<Master, Void>()
    {
        @Override
        public Response<Void> call( Master master, RequestContext context, ChannelBuffer input,
                ChannelBuffer target )
        {
            return master.initializeTx( context );
        }
    }, VOID_SERIALIZER, true ),

    // ====
    ACQUIRE_GRAPH_WRITE_LOCK( new TargetCaller<Master, LockResult>()
    {
        @Override
        public Response<LockResult> call( Master master, RequestContext context, ChannelBuffer input,
                ChannelBuffer target )
        {
            return master.acquireGraphWriteLock( context );
        }
    }, LOCK_SERIALIZER, true )
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
    }, LOCK_SERIALIZER, true )
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

    }, LOCK_SERIALIZER, true )
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

    }, LOCK_SERIALIZER, true )
    {
        @Override
        public boolean isLock()
        {
            return true;
        }
    };

    @SuppressWarnings( "rawtypes" )
    final TargetCaller caller;
    @SuppressWarnings( "rawtypes" )
    final ObjectSerializer serializer;
    private final boolean includesSlaveContext;

    private <T> HaRequestType17( TargetCaller caller, ObjectSerializer<T> serializer, boolean includesSlaveContext )
    {
        this.caller = caller;
        this.serializer = serializer;
        this.includesSlaveContext = includesSlaveContext;
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

    public boolean includesSlaveContext()
    {
        return this.includesSlaveContext;
    }

    public boolean isLock()
    {
        return false;
    }
}