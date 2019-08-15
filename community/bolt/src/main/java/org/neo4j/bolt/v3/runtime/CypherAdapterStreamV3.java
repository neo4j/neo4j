/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.bolt.v3.runtime;

import java.time.Clock;

import org.neo4j.bolt.runtime.statemachine.impl.BoltAdapterSubscriber;
import org.neo4j.bolt.runtime.AbstractCypherAdapterStream;
import org.neo4j.kernel.impl.query.QueryExecution;

import static org.neo4j.values.storable.Values.longValue;

public class CypherAdapterStreamV3 extends AbstractCypherAdapterStream
{
    private static final String LAST_RESULT_CONSUMED_KEY = "t_last";

    public CypherAdapterStreamV3( QueryExecution delegate, BoltAdapterSubscriber subscriber,  Clock clock )
    {
        super( delegate, subscriber, clock );
    }

    @Override
    protected void addDatabaseName( RecordConsumer recordConsumer )
    {
        // on purpose left empty
    }

    @Override
    protected void addRecordStreamingTime( long time, RecordConsumer recordConsumer )
    {
        recordConsumer.addMetadata( LAST_RESULT_CONSUMED_KEY, longValue( time ) );
    }
}
