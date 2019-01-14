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
package org.neo4j.kernel.ha.com.slave;

import org.neo4j.com.ComExceptionHandler;
import org.neo4j.com.Deserializer;
import org.neo4j.com.ObjectSerializer;
import org.neo4j.com.ProtocolVersion;
import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.storecopy.ResponseUnpacker.TxHandler;
import org.neo4j.com.storecopy.StoreWriter;
import org.neo4j.kernel.ha.MasterClient320;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.lock.LockResult;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;

public interface MasterClient extends Master
{
    ProtocolVersion CURRENT = MasterClient320.PROTOCOL_VERSION;

    @Override
    Response<Integer> createRelationshipType( RequestContext context, String name );

    @Override
    Response<Void> newLockSession( RequestContext context );

    @Override
    Response<Long> commit( RequestContext context, TransactionRepresentation channel );

    @Override
    Response<Void> pullUpdates( RequestContext context );

    Response<Void> pullUpdates( RequestContext context, TxHandler txHandler );

    @Override
    Response<Void> copyStore( RequestContext context, StoreWriter writer );

    void setComExceptionHandler( ComExceptionHandler handler );

    ProtocolVersion getProtocolVersion();

    ObjectSerializer<LockResult> createLockResultSerializer();

    Deserializer<LockResult> createLockResultDeserializer();
}
