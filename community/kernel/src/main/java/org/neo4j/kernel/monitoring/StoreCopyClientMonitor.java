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
package org.neo4j.kernel.monitoring;

public interface StoreCopyClientMonitor
{
    void startReceivingStoreFiles();
    void finishedReceivingStoreFiles();
    void startReceivingTransactions( long startTxId );
    void finishReceivingTransactions( long endTxId );
    void startRecoveringStore();
    void finishRecoveringStore();

    public static final StoreCopyClientMonitor NONE = new Adaptor();

    class Adaptor implements StoreCopyClientMonitor {

        @Override
        public void startReceivingStoreFiles()
        {

        }

        @Override
        public void finishedReceivingStoreFiles()
        {

        }

        @Override
        public void startReceivingTransactions( long startTxId )
        {

        }

        @Override
        public void finishReceivingTransactions( long endTxId )
        {

        }

        @Override
        public void startRecoveringStore()
        {

        }

        @Override
        public void finishRecoveringStore()
        {

        }

    }
}
