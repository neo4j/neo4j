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
import org.neo4j.com.TxExtractor;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.lock.LockResult;
import org.neo4j.kernel.ha.lock.LockStatus;

import static org.neo4j.com.Protocol.readString;
import static org.neo4j.com.Protocol.writeString;

public interface MasterClient extends Master
{
    static final ObjectSerializer<LockResult> LOCK_SERIALIZER = new ObjectSerializer<LockResult>()
    {
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
        public LockResult read( ChannelBuffer buffer, ByteBuffer temporaryBuffer ) throws IOException
        {
            LockStatus status = LockStatus.values()[buffer.readByte()];
            return status.hasMessage() ? new LockResult( readString( buffer ) ) : new LockResult( status );
        }
    };

    public Response<Integer> createRelationshipType( RequestContext context, final String name );

    public Response<Void> initializeTx( RequestContext context );

    public Response<LockResult> acquireNodeReadLock( RequestContext context, long... nodes );

    public Response<LockResult> acquireRelationshipWriteLock( RequestContext context, long... relationships );

    public Response<LockResult> acquireRelationshipReadLock( RequestContext context, long... relationships );

    public Response<LockResult> acquireGraphWriteLock( RequestContext context );

    public Response<LockResult> acquireGraphReadLock( RequestContext context );

    public Response<LockResult> acquireIndexReadLock( RequestContext context, String index, String key );

    public Response<LockResult> acquireIndexWriteLock( RequestContext context, String index, String key );

    public Response<Long> commitSingleResourceTransaction( RequestContext context, final String resource,
            final TxExtractor txGetter );

    public Response<Void> finishTransaction( RequestContext context, final boolean success );

    public void rollbackOngoingTransactions( RequestContext context );

    public Response<Void> pullUpdates( RequestContext context );

    public Response<Void> copyStore( RequestContext context, final StoreWriter writer );

    public Response<Void> copyTransactions( RequestContext context, final String ds, final long startTxId,
            final long endTxId );

    public void addMismatchingVersionHandler( MismatchingVersionHandler toAdd );
}
