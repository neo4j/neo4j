/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.transaction.state.storeview;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.neo4j.kernel.impl.api.index.PropertyScanConsumer;
import org.neo4j.values.storable.Value;

import static java.util.Collections.synchronizedList;

public class TestPropertyScanConsumer implements PropertyScanConsumer
{
    public final List<List<Record>> batches = synchronizedList( new ArrayList<>() );

    @Override
    public PropertyScanConsumer.Batch newBatch()
    {
        return new Batch()
        {
            final List<Record> batchEntityUpdates = new ArrayList<>();

            @Override
            public void addRecord( long entityId, long[] tokens, Map<Integer,Value> properties )
            {
                batchEntityUpdates.add( new Record( entityId, tokens, properties ) );
            }

            @Override
            public void process()
            {
                batches.add( batchEntityUpdates );
            }
        };
    }

    public static class Record
    {
        private final long entityId;
        private final long[] tokens;
        private final Map<Integer,Value> properties;

        public Record( long entityId, long[] tokens, Map<Integer,Value> properties )
        {
            this.entityId = entityId;
            this.tokens = tokens;
            this.properties = properties;
        }

        public long getEntityId()
        {
            return entityId;
        }

        public long[] getTokens()
        {
            return tokens;
        }

        public Map<Integer,Value> getProperties()
        {
            return properties;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            Record record = (Record) o;
            return entityId == record.entityId &&
                    Arrays.equals( tokens, record.tokens ) &&
                    properties.equals( record.properties );
        }

        @Override
        public int hashCode()
        {
            int result = Objects.hash( entityId, properties );
            result = 31 * result + Arrays.hashCode( tokens );
            return result;
        }
    }
}
