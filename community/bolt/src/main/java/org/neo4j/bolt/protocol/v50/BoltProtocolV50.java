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
package org.neo4j.bolt.protocol.v50;

import java.util.Set;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.connector.connection.Feature;
import org.neo4j.bolt.protocol.io.pipeline.WriterPipeline;
import org.neo4j.bolt.protocol.io.reader.DateReader;
import org.neo4j.bolt.protocol.io.reader.DateTimeReader;
import org.neo4j.bolt.protocol.io.reader.DateTimeZoneIdReader;
import org.neo4j.bolt.protocol.io.reader.DurationReader;
import org.neo4j.bolt.protocol.io.reader.LocalDateTimeReader;
import org.neo4j.bolt.protocol.io.reader.LocalTimeReader;
import org.neo4j.bolt.protocol.io.reader.Point2dReader;
import org.neo4j.bolt.protocol.io.reader.Point3dReader;
import org.neo4j.bolt.protocol.io.reader.TimeReader;
import org.neo4j.bolt.protocol.io.writer.DefaultStructWriter;
import org.neo4j.bolt.protocol.v44.BoltProtocolV44;
import org.neo4j.bolt.transaction.TransactionManager;
import org.neo4j.kernel.database.DefaultDatabaseResolver;
import org.neo4j.logging.internal.LogService;
import org.neo4j.packstream.struct.StructRegistry;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.values.storable.Value;

public class BoltProtocolV50 extends BoltProtocolV44 {
    public static final ProtocolVersion VERSION = new ProtocolVersion(5, 0);

    public BoltProtocolV50(
            LogService logging,
            BoltGraphDatabaseManagementServiceSPI boltGraphDatabaseManagementServiceSPI,
            DefaultDatabaseResolver defaultDatabaseResolver,
            TransactionManager transactionManager,
            SystemNanoClock clock) {
        super(logging, boltGraphDatabaseManagementServiceSPI, defaultDatabaseResolver, transactionManager, clock);
    }

    @Override
    public ProtocolVersion version() {
        return VERSION;
    }

    @Override
    public Set<Feature> features() {
        return Set.of(Feature.UTC_DATETIME);
    }

    @Override
    public void registerStructWriters(WriterPipeline pipeline) {
        pipeline.addLast(DefaultStructWriter.getInstance());
    }

    @Override
    public void registerStructReaders(StructRegistry.Builder<Connection, Value> builder) {
        // TODO: Protocols should no longer need to inherit from each other in order of release.
        //       Provide a base type per major release instead?
        builder.register(DateReader.getInstance())
                .register(DurationReader.getInstance())
                .register(LocalDateTimeReader.getInstance())
                .register(LocalTimeReader.getInstance())
                .register(Point2dReader.getInstance())
                .register(Point3dReader.getInstance())
                .register(TimeReader.getInstance())
                .register(DateTimeReader.getInstance())
                .register(DateTimeZoneIdReader.getInstance());
    }
}
