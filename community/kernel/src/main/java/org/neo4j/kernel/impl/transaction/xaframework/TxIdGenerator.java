/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

public interface TxIdGenerator
{
    public static final TxIdGenerator DEFAULT = new TxIdGenerator()
    {
        public long generate( XaDataSource dataSource, int identifier )
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
    };
    
    long generate( XaDataSource dataSource, int identifier );
    
    int getCurrentMasterId();

    int getMyId();
}
