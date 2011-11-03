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
package org.neo4j.kernel.impl.util;

import java.io.IOException;

import org.neo4j.kernel.Config;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.transaction.xaframework.InMemoryLogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog.LogExtractor;

public class CompareTxStreams
{
    public static void main( String[] args ) throws IOException
    {
        EmbeddedGraphDatabase db1 = new EmbeddedGraphDatabase( args[0] );
        EmbeddedGraphDatabase db2 = new EmbeddedGraphDatabase( args[1] );
        
        try
        {
            LogExtractor extractor1 = getLogExtractor( db1 );
            LogExtractor extractor2 = getLogExtractor( db2 );
            boolean branchingDetected = false;
            long lastTx = 1;
            while ( true )
            {
                long tx1 = extractor1.extractNext( new InMemoryLogBuffer() );
                long tx2 = extractor2.extractNext( new InMemoryLogBuffer() );
                if ( tx1 != tx2 ) throw new RuntimeException( "Differing tx " + tx1 + " and " + tx2 );
                if ( tx1 == -1 || tx2 == -1 ) break;
                lastTx = tx1;
                if ( !branchingDetected )
                {   // Try to detect branching
                    if ( extractor1.getLastStartEntry().getMasterId() != extractor2.getLastStartEntry().getMasterId() ||
                            extractor1.getLastTxChecksum() != extractor2.getLastTxChecksum() )
                    {
                        branchingDetected = true;
                        System.out.println( "Branch at " + tx1 + ": masters:" + extractor1.getLastStartEntry().getMasterId() + "," + extractor2.getLastStartEntry().getMasterId() +
                                " checksums:" + extractor1.getLastTxChecksum() + "," + extractor2.getLastTxChecksum() );
                    }
                }
                else
                {   // Try to detect merging of branch
                    if ( extractor1.getLastStartEntry().getMasterId() == extractor2.getLastStartEntry().getMasterId() &&
                            extractor1.getLastTxChecksum() == extractor2.getLastTxChecksum() )
                    {
                        branchingDetected = false;
                        System.out.println( "Merged again at " + tx1 );
                    }
                    else
                    {
                        System.out.println( "Still branched at " + tx1 + ": masters:" + extractor1.getLastStartEntry().getMasterId() + "," + extractor2.getLastStartEntry().getMasterId() +
                                " checksums:" + extractor1.getLastTxChecksum() + "," + extractor2.getLastTxChecksum() );
                    }
                }
            }
            System.out.println( "Last tx " + lastTx );
        }
        finally
        {
            db1.shutdown();
            db2.shutdown();
        }
    }
    
    private static LogExtractor getLogExtractor( EmbeddedGraphDatabase db ) throws IOException
    {
        XaDataSource ds = db.getConfig().getTxModule().getXaDataSourceManager().getXaDataSource( Config.DEFAULT_DATA_SOURCE_NAME );
        return ds.getLogExtractor( 2, ds.getLastCommittedTxId() );
    }
}
