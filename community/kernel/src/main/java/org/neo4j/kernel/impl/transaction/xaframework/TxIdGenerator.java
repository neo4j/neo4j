/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.xaframework;

import javax.transaction.xa.XAException;

public interface TxIdGenerator
{
    TxIdGenerator DEFAULT = new TxIdGenerator()
    {
        public long generate( XaDataSource dataSource, int identifier ) throws XAException
        {
            return dataSource.getLastCommittedTxId() + 1;
        }
        
        public int getCurrentMasterId()
        {
            return XaLogicalLog.MASTER_ID_REPRESENTING_NO_MASTER;
        }
        
        public int getMyId()
        {
            return XaLogicalLog.MASTER_ID_REPRESENTING_NO_MASTER;
        }

        @Override
        public void committed( XaDataSource dataSource, int identifier, long txId, Integer externalAuthor )
        {
        }
    };
    
    /**
     * Generates a transaction id to use for the committing transaction.
     *
     * @param dataSource {@link org.neo4j.kernel.impl.transaction.xaframework.XaDataSource} to commit.
     * @param identifier temporary transaction identifier.
     * @return transaction id to use to commit the next transaction for
     * this {@code dataSource}.
     */
    long generate( final XaDataSource dataSource, final int identifier ) throws XAException;
    
    /**
     * Hook which gets called when a transaction has been committed, but before
     * returning from commit methods.
     * @param dataSource {@link XaDataSource} which committed the transaction.
     * @param identifier temporary identifier for the committed transaction.
     * @param txId the transaction id used for this committed transaction.
     * @param externalAuthorServerId if this transaction was authored by an
     * external server exclude it as push target for this transaction.
     * {@code null} means authored by this server.
     */
    void committed( XaDataSource dataSource, int identifier, long txId, Integer externalAuthorServerId );
    
    /**
     * Returns the id of the current master. For single instance case it's
     * {@code -1}, but in multi instance scenario it returns the id
     * of the current master instance.
     * @return id of the current master.
     */
    int getCurrentMasterId();

    /**
     * Returns the id of my database instance. In a single instance scenario
     * {@code -1} will be returned. 
     * @return my database instance id.
     */
    int getMyId();
}
