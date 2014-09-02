/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.ha.com.slave;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.jboss.netty.buffer.ChannelBuffer;

import org.neo4j.com.Deserializer;
import org.neo4j.com.MismatchingVersionHandler;
import org.neo4j.com.ObjectSerializer;
import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.storecopy.StoreWriter;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.lock.LockResult;
import org.neo4j.kernel.ha.lock.LockStatus;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionRepresentation;

import static java.lang.String.format;

import static org.neo4j.com.Protocol.readString;
import static org.neo4j.com.Protocol.writeString;

public interface MasterClient extends Master
{
    static final ObjectSerializer<LockResult> LOCK_SERIALIZER = new ObjectSerializer<LockResult>()
    {
        @Override
        public void write( LockResult responseObject, ChannelBuffer result ) throws IOException
        {
            result.writeByte( responseObject.getStatus().ordinal() );
            if ( responseObject.getStatus().hasMessage() )
            {
                writeString( result, responseObject.getDeadlockMessage() );
            }
        }
    };

    static final Deserializer<LockResult> LOCK_RESULT_DESERIALIZER = new Deserializer<LockResult>()
    {
        @Override
        public LockResult read( ChannelBuffer buffer, ByteBuffer temporaryBuffer ) throws IOException
        {
            byte statusOrdinal = buffer.readByte();
            LockStatus status;
            try
            {
                status = LockStatus.values()[statusOrdinal];
            }
            catch ( ArrayIndexOutOfBoundsException e )
            {
                throw Exceptions.withMessage( e, e.getMessage() + " | read invalid ordinal " + statusOrdinal +
                        ". The whole contents of this channel buffer is " + wholeBufferAsString( buffer ) );
            }
            return status.hasMessage() ? new LockResult( readString( buffer ) ) : new LockResult( status );
        }

        private String wholeBufferAsString( ChannelBuffer buffer )
        {
            int prevIndex = buffer.readerIndex();
            try
            {
                buffer.readerIndex( 0 );
                StringBuilder builder = new StringBuilder();
                for ( int i = 0; buffer.readable(); i++ )
                {
                    byte value = buffer.readByte();
                    builder.append( i > 0 ? "," : "" ).append( format( "%x", value ) );
                }
                return builder.toString();
            }
            finally
            {
                buffer.readerIndex( prevIndex );
            }
        }
    };

    @Override
    public Response<Integer> createRelationshipType( RequestContext context, final String name );

    @Override
    public Response<Void> newLockSession( RequestContext context );

    @Override
    public Response<Long> commit( RequestContext context, final TransactionRepresentation channel );

    @Override
    public Response<Void> endLockSession( RequestContext context, final boolean success );

    public void rollbackOngoingTransactions( RequestContext context );

    @Override
    public Response<Void> pullUpdates( RequestContext context );

    @Override
    public Response<Void> copyStore( RequestContext context, final StoreWriter writer );

    public void addMismatchingVersionHandler( MismatchingVersionHandler toAdd );
}
