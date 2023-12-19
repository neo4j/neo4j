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
package org.neo4j.com.storecopy;

public interface StoreCopyClientMonitor
{
    void startReceivingStoreFiles();

    void finishReceivingStoreFiles();

    void startReceivingStoreFile( String file );

    void finishReceivingStoreFile( String file );

    void startReceivingTransactions( long startTxId );

    void finishReceivingTransactions( long endTxId );

    void startRecoveringStore();

    void finishRecoveringStore();

    void startReceivingIndexSnapshots();

    void startReceivingIndexSnapshot( long indexId );

    void finishReceivingIndexSnapshot( long indexId );

    void finishReceivingIndexSnapshots();

    class Adapter implements StoreCopyClientMonitor
    {
        @Override
        public void startReceivingStoreFiles()
        {   // empty
        }

        @Override
        public void finishReceivingStoreFiles()
        {   // empty
        }

        @Override
        public void startReceivingStoreFile( String file )
        {   // empty
        }

        @Override
        public void finishReceivingStoreFile( String file )
        {   // empty
        }

        @Override
        public void startReceivingTransactions( long startTxId )
        {   // empty
        }

        @Override
        public void finishReceivingTransactions( long endTxId )
        {   // empty
        }

        @Override
        public void startRecoveringStore()
        {   // empty
        }

        @Override
        public void finishRecoveringStore()
        {   // empty
        }

        @Override
        public void startReceivingIndexSnapshots()
        {   // empty
        }

        @Override
        public void startReceivingIndexSnapshot( long indexId )
        {   // empty
        }

        @Override
        public void finishReceivingIndexSnapshot( long indexId )
        {   // empty
        }

        @Override
        public void finishReceivingIndexSnapshots()
        {   // empty
        }
    }
}
