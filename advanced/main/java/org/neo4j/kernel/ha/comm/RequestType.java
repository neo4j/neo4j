package org.neo4j.kernel.ha.comm;

import org.jboss.netty.buffer.ChannelBuffer;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.ha.IdAllocation;
import org.neo4j.kernel.ha.LockResult;
import org.neo4j.kernel.ha.Master;
import org.neo4j.kernel.ha.Response;
import org.neo4j.kernel.ha.SlaveContext;

public enum RequestType
{
    ALLOCATE_NODE_IDS( false )
    {
        @Override
        public Response<DataWriter> invoke( Master master, SlaveContext context )
        {
            IdAllocation response = master.allocateIds( IdType.NODE );
            DataWriter writer = new DataWriter.WriteIdAllocation( response );
            return Response.wrapResponseObjectOnly( writer );
        }

        @Override
        public Object readResponse( ChannelBuffer buffer )
        {
            return CommunicationUtils.readIdAllocation( buffer );
        }
    },
    ALLOCATE_RELATIONSHIP_IDS( false )
    {
        @Override
        public Response<DataWriter> invoke( Master master, SlaveContext context )
        {
            IdAllocation response = master.allocateIds( IdType.RELATIONSHIP );
            DataWriter writer = new DataWriter.WriteIdAllocation( response );
            return Response.wrapResponseObjectOnly( writer );
        }

        @Override
        public Object readResponse( ChannelBuffer buffer )
        {
            return CommunicationUtils.readIdAllocation( buffer );
        }
    },
    ALLOCATE_PROPERTY_IDS( false )
    {
        @Override
        public Response<DataWriter> invoke( Master master, SlaveContext context )
        {
            IdAllocation response = master.allocateIds( IdType.PROPERTY );
            DataWriter writer = new DataWriter.WriteIdAllocation( response );
            return Response.wrapResponseObjectOnly( writer );
        }

        @Override
        public Object readResponse( ChannelBuffer buffer )
        {
            return CommunicationUtils.readIdAllocation( buffer );
        }
    },
    ALLOCATE_STRING_BLOCK_IDS( false )
    {
        @Override
        public Response<DataWriter> invoke( Master master, SlaveContext context )
        {
            IdAllocation response = master.allocateIds( IdType.STRING_BLOCK );
            DataWriter writer = new DataWriter.WriteIdAllocation( response );
            return Response.wrapResponseObjectOnly( writer );
        }

        @Override
        public Object readResponse( ChannelBuffer buffer )
        {
            return CommunicationUtils.readIdAllocation( buffer );
        }
    },
    ALLOCATE_ARRAY_BLOCK_IDS( false )
    {
        @Override
        public Response<DataWriter> invoke( Master master, SlaveContext context )
        {
            IdAllocation response = master.allocateIds( IdType.ARRAY_BLOCK );
            DataWriter writer = new DataWriter.WriteIdAllocation( response );
            return Response.wrapResponseObjectOnly( writer );
        }

        @Override
        public Object readResponse( ChannelBuffer buffer )
        {
            return CommunicationUtils.readIdAllocation( buffer );
        }
    },
    ALLOCATE_PROPERTY_INDEX_IDS( false )
    {
        @Override
        public Response<DataWriter> invoke( Master master, SlaveContext context )
        {
            IdAllocation response = master.allocateIds( IdType.PROPERTY_INDEX );
            DataWriter writer = new DataWriter.WriteIdAllocation( response );
            return Response.wrapResponseObjectOnly( writer );
        }

        @Override
        public Object readResponse( ChannelBuffer buffer )
        {
            return CommunicationUtils.readIdAllocation( buffer );
        }
    },
    ALLOCATE_PROPERTY_INDEX_BLOCK_IDS( false )
    {
        @Override
        public Response<DataWriter> invoke( Master master, SlaveContext context )
        {
            IdAllocation response = master.allocateIds( IdType.PROPERTY_INDEX_BLOCK );
            DataWriter writer = new DataWriter.WriteIdAllocation( response );
            return Response.wrapResponseObjectOnly( writer );
        }

        @Override
        public Object readResponse( ChannelBuffer buffer )
        {
            return CommunicationUtils.readIdAllocation( buffer );
        }
    },
    ALLOCATE_RELATIONSHIP_TYPE_IDS( false )
    {
        @Override
        public Response<DataWriter> invoke( Master master, SlaveContext context )
        {
            IdAllocation response = master.allocateIds( IdType.RELATIONSHIP_TYPE );
            DataWriter writer = new DataWriter.WriteIdAllocation( response );
            return Response.wrapResponseObjectOnly( writer );
        }

        @Override
        public Object readResponse( ChannelBuffer buffer )
        {
            return CommunicationUtils.readIdAllocation( buffer );
        }
    },
    ALLOCATE_RELATIONSHIP_TYPE_BLOCK_IDS( false )
    {
        @Override
        public Response<DataWriter> invoke( Master master, SlaveContext context )
        {
            IdAllocation response = master.allocateIds( IdType.RELATIONSHIP_TYPE_BLOCK );
            DataWriter writer = new DataWriter.WriteIdAllocation( response );
            return Response.wrapResponseObjectOnly( writer );
        }

        @Override
        public Object readResponse( ChannelBuffer buffer )
        {
            return CommunicationUtils.readIdAllocation( buffer );
        }
    },
    ALLOCATE_NEOSTORE_BLOCK_IDS( false )
    {
        @Override
        public Response<DataWriter> invoke( Master master, SlaveContext context )
        {
            IdAllocation response = master.allocateIds( IdType.NEOSTORE_BLOCK );
            DataWriter writer = new DataWriter.WriteIdAllocation( response );
            return Response.wrapResponseObjectOnly( writer );
        }

        @Override
        public Object readResponse( ChannelBuffer buffer )
        {
            return CommunicationUtils.readIdAllocation( buffer );
        }
    },
    CREATE_RELATIONSHIP_TYPE( true )
    {
        @Override
        Object readRequest( ChannelBuffer buffer, final SlaveContext context )
        {
            if ( !buffer.readable() ) return null;
            int length = buffer.readUnsignedByte() * 2;
            if ( buffer.readableBytes() < length ) return null;
            final String name = CommunicationUtils.readString( buffer, length / 2 );
            return new MasterInvoker()
            {
                @SuppressWarnings( "boxing" )
                public Response<DataWriter> invoke( Master master )
                {
                    Response<Integer> response = master.createRelationshipType( context, name );
                    DataWriter writer = new DataWriter.WriteInt( response.response() );
                    return new Response<DataWriter>( writer, response.transactions() );
                }
            };
        }

        @Override
        @SuppressWarnings( "boxing" )
        public Object readResponse( ChannelBuffer buffer )
        {
            return buffer.readInt();
        }
    },
    ACQUIRE_NODE_WRITE_LOCK( true )
    {
        @Override
        Object readRequest( ChannelBuffer buffer, final SlaveContext context )
        {
            final long[] ids = CommunicationUtils.tryReadIdArray( buffer );
            if ( ids == null ) return null;
            return new MasterInvoker()
            {
                public Response<DataWriter> invoke( Master master )
                {
                    Response<LockResult> response = master.acquireNodeWriteLock( context, ids );
                    return new Response<DataWriter>( new DataWriter.WriteLockResult(
                            response.response() ), response.transactions() );
                }
            };
        }

        @Override
        public Object readResponse( ChannelBuffer buffer )
        {
            return CommunicationUtils.readLockResult( buffer );
        }
    },
    ACQUIRE_NODE_READ_LOCK( true )
    {
        @Override
        Object readRequest( ChannelBuffer buffer, final SlaveContext context )
        {
            final long[] ids = CommunicationUtils.tryReadIdArray( buffer );
            if ( ids == null ) return null;
            return new MasterInvoker()
            {

                public Response<DataWriter> invoke( Master master )
                {
                    Response<LockResult> response = master.acquireNodeReadLock( context, ids );
                    return new Response<DataWriter>( new DataWriter.WriteLockResult(
                            response.response() ), response.transactions() );
                }
            };
        }

        @Override
        public Object readResponse( ChannelBuffer buffer )
        {
            return CommunicationUtils.readLockResult( buffer );
        }
    },
    ACQUIRE_RELATIONSHIP_WRITE_LOCK( true )
    {
        @Override
        Object readRequest( ChannelBuffer buffer, final SlaveContext context )
        {
            final long[] ids = CommunicationUtils.tryReadIdArray( buffer );
            if ( ids == null ) return null;
            return new MasterInvoker()
            {
                public Response<DataWriter> invoke( Master master )
                {
                    Response<LockResult> response = master.acquireRelationshipWriteLock( context,
                            ids );
                    return new Response<DataWriter>( new DataWriter.WriteLockResult(
                            response.response() ), response.transactions() );
                }
            };
        }

        @Override
        public Object readResponse( ChannelBuffer buffer )
        {
            return CommunicationUtils.readLockResult( buffer );
        }
    },
    ACQUIRE_RELATIONSHIP_READ_LOCK( true )
    {
        @Override
        Object readRequest( ChannelBuffer buffer, final SlaveContext context )
        {
            final long[] ids = CommunicationUtils.tryReadIdArray( buffer );
            if ( ids == null ) return null;
            return new MasterInvoker()
            {
                public Response<DataWriter> invoke( Master master )
                {
                    Response<LockResult> response = master.acquireRelationshipReadLock( context,
                            ids );
                    return new Response<DataWriter>( new DataWriter.WriteLockResult(
                            response.response() ), response.transactions() );
                }
            };
        }

        @Override
        public Object readResponse( ChannelBuffer buffer )
        {
            return CommunicationUtils.readLockResult( buffer );
        }
    },
    COMMIT( true )
    {
        @Override
        Object readRequest( ChannelBuffer buffer, SlaveContext context )
        {
            if ( !buffer.readable() ) return null;
            int numTransactions = buffer.readUnsignedByte();
            if ( numTransactions > 0 )
            {
                return new TransactionDataReader.Multiple( context, numTransactions );
            }
            // numTransactions == 0 is a strange message
            return null;
        }
    },
    PULL_UPDATES( true )
    {
        @Override
        public Response<DataWriter> invoke( Master master, SlaveContext context )
        {
            Response<Void> response = master.pullUpdates( context );
            return new Response<DataWriter>( DataWriter.VOID, response.transactions() );
        }
    },
    FINISH( true )
    {
        @Override
        public Response<DataWriter> invoke( Master master, SlaveContext context )
        {
            Response<Void> response = master.finishTransaction( context );
            return new Response<DataWriter>( DataWriter.VOID, response.transactions() );
        }
    },
    GET_MASTER_ID_FOR_TX( false )
    {
        @Override
        Object readRequest( ChannelBuffer buffer, final SlaveContext context )
        {
            if ( buffer.readableBytes() < 8 ) return null;
            final long localTxId = buffer.readLong();
            return new MasterInvoker()
            {
                public Response<DataWriter> invoke( Master master )
                {
                    int masterId = master.getMasterIdForCommittedTx( localTxId );
                    DataWriter writer = new DataWriter.WriteInt( masterId );
                    return Response.wrapResponseObjectOnly( writer );
                }
            };
        }

        @Override
        @SuppressWarnings( "boxing" )
        public Object readResponse( ChannelBuffer buffer )
        {
            return buffer.readInt();
        }
    },
    ;
    final boolean includesSlaveContext;

    private RequestType( boolean includesSlaveContext )
    {
        this.includesSlaveContext = includesSlaveContext;
    }

    public final Object readRequest( ChannelBuffer buffer )
    {
        SlaveContext context = null;
        if ( includesSlaveContext )
        {
            context = CommunicationUtils.tryReadSlaveContext( buffer );
            if ( context == null ) return null;
        }
        return readRequest( buffer, context );
    }

    Object readRequest( ChannelBuffer buffer, final SlaveContext context )
    {
        return new MasterInvoker()
        {
            public Response<DataWriter> invoke( Master master )
            {
                return RequestType.this.invoke( master, context );
            }
        };
    }

    public Object readResponse( ChannelBuffer buffer )
    {
        return null; // default: read void
    }

    public Response<DataWriter> invoke( Master master, SlaveContext context )
    {
        throw new Error( "MasterInvoker not implemented for " + name() );
    }

    public static RequestType get( int code )
    {
        return values()[code];
    }

    public static RequestType allocateIds( IdType type )
    {
        switch ( type )
        {
        case NODE:
            return ALLOCATE_NODE_IDS;
        case RELATIONSHIP:
            return ALLOCATE_RELATIONSHIP_IDS;
        case PROPERTY:
            return ALLOCATE_PROPERTY_IDS;
        case STRING_BLOCK:
            return ALLOCATE_STRING_BLOCK_IDS;
        case ARRAY_BLOCK:
            return ALLOCATE_ARRAY_BLOCK_IDS;
        case PROPERTY_INDEX:
            return ALLOCATE_PROPERTY_INDEX_IDS;
        case PROPERTY_INDEX_BLOCK:
            return ALLOCATE_PROPERTY_INDEX_BLOCK_IDS;
        case RELATIONSHIP_TYPE:
            return ALLOCATE_RELATIONSHIP_TYPE_IDS;
        case RELATIONSHIP_TYPE_BLOCK:
            return ALLOCATE_RELATIONSHIP_TYPE_BLOCK_IDS;
        case NEOSTORE_BLOCK:
            return ALLOCATE_NEOSTORE_BLOCK_IDS;
        default:
            throw new IllegalArgumentException( "Don't know how to allocate " + type.name() );
        }
    }
}
