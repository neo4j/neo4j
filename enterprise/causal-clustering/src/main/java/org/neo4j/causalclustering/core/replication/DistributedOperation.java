/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.core.replication;

import java.io.IOException;
import java.util.UUID;

import org.neo4j.causalclustering.messaging.CoreReplicatedContentMarshal;
import org.neo4j.causalclustering.core.replication.session.GlobalSession;
import org.neo4j.causalclustering.core.replication.session.LocalOperationId;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

/**
 * A uniquely identifiable operation.
 */
public class  DistributedOperation implements ReplicatedContent
{
    private final ReplicatedContent content;
    private final GlobalSession globalSession;
    private final LocalOperationId operationId;

    public DistributedOperation( ReplicatedContent content, GlobalSession globalSession, LocalOperationId operationId )
    {
        this.content = content;
        this.globalSession = globalSession;
        this.operationId = operationId;
    }

    public GlobalSession globalSession()
    {
        return globalSession;
    }

    public LocalOperationId operationId()
    {
        return operationId;
    }

    public ReplicatedContent content()
    {
        return content;
    }

    @Override
    public boolean hasSize()
    {
        return content.hasSize();
    }

    @Override
    public long size()
    {
        return content.size();
    }

    public void serialize( WritableChannel channel ) throws IOException
    {
        channel.putLong( globalSession().sessionId().getMostSignificantBits() );
        channel.putLong( globalSession().sessionId().getLeastSignificantBits() );
        new MemberId.Marshal().marshal( globalSession().owner(), channel );

        channel.putLong( operationId.localSessionId() );
        channel.putLong( operationId.sequenceNumber() );

        new CoreReplicatedContentMarshal().marshal( content, channel );
    }

    public static DistributedOperation deserialize( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        long mostSigBits = channel.getLong();
        long leastSigBits = channel.getLong();
        MemberId owner = new MemberId.Marshal().unmarshal( channel );
        GlobalSession globalSession = new GlobalSession( new UUID( mostSigBits, leastSigBits ), owner );

        long localSessionId = channel.getLong();
        long sequenceNumber = channel.getLong();
        LocalOperationId localOperationId = new LocalOperationId( localSessionId, sequenceNumber );

        ReplicatedContent content = new CoreReplicatedContentMarshal().unmarshal( channel );
        return new DistributedOperation( content, globalSession, localOperationId );
    }

    @Override
    public String toString()
    {
        return "DistributedOperation{" +
               "content=" + content +
               ", globalSession=" + globalSession +
               ", operationId=" + operationId +
               '}';
    }
}
