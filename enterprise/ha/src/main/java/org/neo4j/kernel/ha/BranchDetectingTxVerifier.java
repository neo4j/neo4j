/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.kernel.ha;

import java.io.IOException;

import org.neo4j.com.TxChecksumVerifier;
import org.neo4j.kernel.impl.transaction.log.NoSuchTransactionException;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * Used on the master to verify that slaves are using the same logical database as the master is running. This is done
 * by verifying transaction checksums.
 */
public class BranchDetectingTxVerifier implements TxChecksumVerifier
{
    private final Log log;
    private final TransactionChecksumLookup txChecksumLookup;

    public BranchDetectingTxVerifier( LogProvider logProvider, TransactionChecksumLookup txChecksumLookup )
    {
        this.log = logProvider.getLog( getClass() );
        this.txChecksumLookup = txChecksumLookup;
    }

    @Override
    public void assertMatch( long txId, long checksum )
    {
        if ( txId == 0 )
        {
            return;
        }
        long readChecksum;
        try
        {
            readChecksum = txChecksumLookup.lookup( txId );
        }
        catch ( NoSuchTransactionException e )
        {
            // This can happen if it's the first commit from a slave in a cluster where all instances
            // just restored from backup (i.e. no previous tx logs exist), OR if a reporting instance
            // is so far behind that logs have been pruned to the point where the slave cannot catch up anymore.
            // In the first case it's fine (slave had to do checksum match to join cluster), and the second case
            // it's an operational issue solved by making sure enough logs are retained.

            return; // Ok!
        }
        catch ( IOException e )
        {
            log.error( "Couldn't verify checksum for " + stringify( txId, checksum ), e );
            throw new BranchedDataException( "Unable to perform a mandatory sanity check due to an IO error.", e );
        }

        if ( checksum != readChecksum )
        {
            throw new BranchedDataException(
                    "The cluster contains two logically different versions of the database. " +
                            "This will be automatically resolved. Details: " + stringify( txId, checksum ) +
                            " does not match " + readChecksum );
        }
    }

    private String stringify( long txId, long checksum )
    {
        return "txId:" + txId + ", checksum:" + checksum;
    }
}
