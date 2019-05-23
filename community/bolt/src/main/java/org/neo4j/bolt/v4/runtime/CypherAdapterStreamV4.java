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
package org.neo4j.bolt.v4.runtime;

import java.time.Clock;

import org.neo4j.bolt.v1.runtime.TransactionStateMachineV1SPI;
import org.neo4j.bolt.v3.runtime.CypherAdapterStreamV3;
import org.neo4j.kernel.impl.query.QueryExecution;

import static org.neo4j.bolt.v4.messaging.MessageMetadataParser.DB_NAME_KEY;
import static org.neo4j.values.storable.Values.stringValue;

public class CypherAdapterStreamV4 extends CypherAdapterStreamV3
{
    private final String databaseName;

    public CypherAdapterStreamV4( QueryExecution delegate, TransactionStateMachineV1SPI.BoltAdapterSubscriber subscriber, Clock clock, String databaseName )
    {
        super( delegate, subscriber, clock );
        this.databaseName = databaseName;
    }

    @Override
    protected void addDatabaseName( RecordConsumer recordConsumer )
    {
        recordConsumer.addMetadata( DB_NAME_KEY, stringValue( databaseName ) );
    }
}
