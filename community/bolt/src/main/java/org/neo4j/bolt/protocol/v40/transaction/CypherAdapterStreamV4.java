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
package org.neo4j.bolt.protocol.v40.transaction;

import static org.neo4j.bolt.protocol.v40.messaging.util.MessageMetadataParserV40.DB_NAME_KEY;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.utf8Value;

import java.time.Clock;
import org.neo4j.bolt.protocol.common.transaction.AbstractCypherAdapterStream;
import org.neo4j.bolt.protocol.common.transaction.result.AdaptingBoltQuerySubscriber;
import org.neo4j.kernel.impl.query.QueryExecution;

public class CypherAdapterStreamV4 extends AbstractCypherAdapterStream {
    private static final String LAST_RESULT_CONSUMED_KEY = "t_last";

    private final String databaseName;

    public CypherAdapterStreamV4(
            QueryExecution delegate, AdaptingBoltQuerySubscriber subscriber, Clock clock, String databaseName) {
        super(delegate, subscriber, clock);
        this.databaseName = databaseName;
    }

    @Override
    protected void addRecordStreamingTime(long time, RecordConsumer recordConsumer) {
        recordConsumer.addMetadata(LAST_RESULT_CONSUMED_KEY, longValue(time));
    }

    @Override
    protected void addDatabaseName(RecordConsumer recordConsumer) {
        recordConsumer.addMetadata(DB_NAME_KEY, utf8Value(databaseName));
    }
}
