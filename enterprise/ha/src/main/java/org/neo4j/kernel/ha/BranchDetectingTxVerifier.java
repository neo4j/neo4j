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
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Provider;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.util.StringLogger;

public class BranchDetectingTxVerifier implements TxChecksumVerifier
{
    private final StringLogger logger;
    private Provider<XaDataSource> xaDataSourceProvider;
    private XaDataSource dataSource;

    public BranchDetectingTxVerifier(StringLogger logger, Provider<XaDataSource> xaDataSourceProvider )
    {
        this.logger = logger;
        this.xaDataSourceProvider = xaDataSourceProvider;
    }
    
    @Override
    public void assertMatch( long txId, int masterId, long checksum )
    {
        try
        {
            Pair<Integer, Long> readChecksum = dataSource().getMasterForCommittedTx( txId );
            boolean match = masterId == readChecksum.first() && checksum == readChecksum.other();
            
            if ( !match )
            {
                throw new BranchedDataException( stringify( txId, masterId, checksum ) +
                        " doesn't match " + readChecksum );
            }
        }
        catch ( IOException e )
        {
            logger.logMessage( "Couldn't verify checksum for " + stringify( txId, masterId, checksum ), e );
            throw new BranchedDataException( e );
        }
    }
    
    private XaDataSource dataSource()
    {
        if ( dataSource == null )
        {
            dataSource = xaDataSourceProvider.instance();
        }
        return dataSource;
    }

    private String stringify( long txId, int masterId, long checksum )
    {
        return "txId:" + txId + ", masterId:" + masterId + ", checksum:" + checksum;
    }
}
