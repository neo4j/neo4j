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
package org.neo4j.kernel.ha;

import java.io.IOException;

import org.neo4j.com.TxChecksumVerifier;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.Logging;

public class BranchDetectingTxVerifier implements TxChecksumVerifier
{
    private final StringLogger logger;
    private XaDataSource dataSource;
    private DependencyResolver resolver;

    public BranchDetectingTxVerifier( DependencyResolver resolver /* I'd like to get in StringLogger, XaDataSource instead */ )
    {
        this.resolver = resolver;
        /* We cannot pass in XaResourceManager because it this time we don't have a
         * proper db, merely the HA graph db which is a layer around a not-yet-started db
         * Rickards restructuring will of course fix this */
        this.logger = resolver.resolveDependency( Logging.class ).getMessagesLog( getClass() );
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
            dataSource = resolver.resolveDependency( XaDataSourceManager.class )
                    .getXaDataSource( NeoStoreXaDataSource.DEFAULT_DATA_SOURCE_NAME );
        }
        return dataSource;
    }

    private String stringify( long txId, int masterId, long checksum )
    {
        return "txId:" + txId + ", masterId:" + masterId + ", checksum:" + checksum;
    }
}
