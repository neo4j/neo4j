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
import org.neo4j.kernel.impl.transaction.xaframework.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionMetadataCache;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * Used on the master to verify that slaves are using the same logical database as the master is running. This is done
 * by verifying transaction checksums.
 */
public class BranchDetectingTxVerifier implements TxChecksumVerifier
{
    private final StringLogger logger;
    private final LogicalTransactionStore txStore;

    public BranchDetectingTxVerifier( StringLogger logger, LogicalTransactionStore logicalTransactionStore)
    {
        this.logger = logger;
        this.txStore = logicalTransactionStore;
    }

    @Override
    public void assertMatch( long txId, int masterId, long checksum )
    {
        if ( txId == 0 )
        {
            return;
        }
        TransactionMetadataCache.TransactionMetadata metadata = null;
        try
        {
            metadata = txStore.getMetadataFor( txId );
        }
        catch ( IOException e )
        {
            logger.logMessage( "Couldn't verify checksum for " + stringify( txId, masterId, checksum ), e );
            throw new BranchedDataException( "Unable to perform a mandatory sanity check due to an IO error.", e );
        }
        int readMaster = metadata.getMasterId();
        long readChecksum = metadata.getChecksum();
        boolean match = masterId == readMaster && checksum == readChecksum;

        if ( !match )
        {
            throw new BranchedDataException(
                    "The cluster contains two logically different versions of the database. " +
                            "This will be automatically resolved. Details: " + stringify( txId, masterId, checksum ) +
                            " does not match " + readChecksum );
        }
    }

    private String stringify( long txId, int masterId, long checksum )
    {
        return "txId:" + txId + ", masterId:" + masterId + ", checksum:" + checksum;
    }
}
