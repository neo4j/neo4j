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
package org.neo4j.kernel.ha;

import java.io.IOException;

import org.neo4j.com.TxChecksumVerifier;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionMetadataCache;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.Logging;

public class BranchDetectingTxVerifier implements TxChecksumVerifier
{
    private final StringLogger logger;
    private DependencyResolver resolver;
    private LogicalTransactionStore txStore;

    public BranchDetectingTxVerifier( DependencyResolver resolver )
    {
        this.resolver = resolver;
        this.logger = resolver.resolveDependency( Logging.class ).getMessagesLog( getClass() );
        this.txStore = resolver.resolveDependency( NeoStoreXaDataSource.class ).getDependencyResolver().
                resolveDependency( LogicalTransactionStore.class );
    }

    @Override
    public void assertMatch( long txId, int masterId, long checksum )
    {
        TransactionMetadataCache.TransactionMetadata metadata = null;
        try
        {
            metadata = txStore.getMetadataFor( txId );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        // TODO 2.2-future this is wrong and I (CG) should feel wrong
        if ( metadata == null )
        {
            System.out.println("Could not find metadata for txid " + txId);
            return;
        }
        int readMaster = metadata.getMasterId();
        long readChecksum = metadata.getChecksum();
        boolean match = masterId == readMaster && checksum == readChecksum;

        if ( !match )
        {
            throw new BranchedDataException( stringify( txId, masterId, checksum ) +
                    " doesn't match " + readChecksum );
        }
    }

    private String stringify( long txId, int masterId, long checksum )
    {
        return "txId:" + txId + ", masterId:" + masterId + ", checksum:" + checksum;
    }
}
