/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.store;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.neo4j.kernel.impl.store.record.NodeRecord;

public class RecordStoreUtil
{
    public static class ReadNodeAnswer implements Answer<NodeRecord>
    {
        private final boolean dense;
        private final long nextRel;
        private final long nextProp;

        public ReadNodeAnswer( boolean dense, long nextRel, long nextProp )
        {
            this.dense = dense;
            this.nextRel = nextRel;
            this.nextProp = nextProp;
        }

        @Override
        public NodeRecord answer( InvocationOnMock invocation ) throws Throwable
        {
            if ( ((Number)invocation.getArguments()[0]).longValue() == 0L && invocation.getArguments()[1] == null &&
                    invocation.getArguments()[2] == null )
            {
                return null;
            }

            NodeRecord record = (NodeRecord) invocation.getArguments()[1];
            record.setId( ((Number)invocation.getArguments()[0]).longValue() );
            record.setInUse( true );
            record.setDense( dense );
            record.setNextRel( nextRel );
            record.setNextProp( nextProp );
            return record;
        }
    }

    public static class NewNodeRecordAnswer implements Answer<NodeRecord>
    {
        @Override
        public NodeRecord answer( InvocationOnMock invocation ) throws Throwable
        {
            return new NodeRecord( -1 );
        }
    }
}
