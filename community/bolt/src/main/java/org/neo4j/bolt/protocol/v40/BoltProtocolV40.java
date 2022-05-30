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
package org.neo4j.bolt.protocol.v40;

import java.util.function.Predicate;
import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.protocol.common.BoltProtocol;
import org.neo4j.bolt.protocol.common.bookmark.BookmarksParser;
import org.neo4j.bolt.protocol.common.connection.BoltConnection;
import org.neo4j.bolt.protocol.common.fsm.StateMachine;
import org.neo4j.bolt.protocol.common.fsm.StateMachineSPIImpl;
import org.neo4j.bolt.protocol.common.message.encoder.FailureMessageEncoder;
import org.neo4j.bolt.protocol.common.message.encoder.IgnoredMessageEncoder;
import org.neo4j.bolt.protocol.common.message.encoder.SuccessMessageEncoder;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.common.message.response.ResponseMessage;
import org.neo4j.bolt.protocol.v40.fsm.StateMachineV40;
import org.neo4j.bolt.protocol.v40.messaging.decoder.BeginMessageDecoder;
import org.neo4j.bolt.protocol.v40.messaging.decoder.CommitMessageDecoder;
import org.neo4j.bolt.protocol.v40.messaging.decoder.DiscardMessageDecoder;
import org.neo4j.bolt.protocol.v40.messaging.decoder.GoodbyeMessageDecoder;
import org.neo4j.bolt.protocol.v40.messaging.decoder.HelloMessageDecoder;
import org.neo4j.bolt.protocol.v40.messaging.decoder.PullMessageDecoder;
import org.neo4j.bolt.protocol.v40.messaging.decoder.ResetMessageDecoder;
import org.neo4j.bolt.protocol.v40.messaging.decoder.RollbackMessageDecoder;
import org.neo4j.bolt.protocol.v40.messaging.decoder.RunMessageDecoder;
import org.neo4j.bolt.protocol.v40.transaction.TransactionStateMachineSPIProviderV4;
import org.neo4j.bolt.transaction.TransactionManager;
import org.neo4j.kernel.database.DefaultDatabaseResolver;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.packstream.signal.FrameSignal;
import org.neo4j.packstream.struct.StructRegistry;
import org.neo4j.time.SystemNanoClock;

/**
 * Bolt protocol V4. It hosts all the components that are specific to BoltV4
 */
public class BoltProtocolV40 implements BoltProtocol {
    public static final ProtocolVersion VERSION = new ProtocolVersion(4, 0);

    protected final LogService logging;
    protected final Log log;

    protected final BoltGraphDatabaseManagementServiceSPI boltGraphDatabaseManagementServiceSPI;
    protected final DefaultDatabaseResolver defaultDatabaseResolver;
    protected final TransactionManager transactionManager;
    protected final SystemNanoClock clock;
    protected final BookmarksParser bookmarksParser;

    public BoltProtocolV40(
            BookmarksParser bookmarksParser,
            LogService logging,
            BoltGraphDatabaseManagementServiceSPI boltGraphDatabaseManagementServiceSPI,
            DefaultDatabaseResolver defaultDatabaseResolver,
            TransactionManager transactionManager,
            SystemNanoClock clock) {
        this.logging = logging;
        this.log = logging.getInternalLog(getClass());

        this.boltGraphDatabaseManagementServiceSPI = boltGraphDatabaseManagementServiceSPI;
        this.defaultDatabaseResolver = defaultDatabaseResolver;
        this.transactionManager = transactionManager;
        this.clock = clock;
        this.bookmarksParser = bookmarksParser;
    }

    @Override
    public ProtocolVersion version() {
        return VERSION;
    }

    @Override
    public Predicate<FrameSignal> frameSignalFilter() {
        // only valid signal in 4.0 was MESSAGE_END - further signals were introduced in later revisions
        return signal -> signal != FrameSignal.MESSAGE_END;
    }

    @Override
    public StateMachine createStateMachine(BoltChannel channel) {
        channel.memoryTracker()
                .allocateHeap(TransactionStateMachineSPIProviderV4.SHALLOW_SIZE
                        + StateMachineSPIImpl.SHALLOW_SIZE
                        + StateMachineV40.SHALLOW_SIZE);

        var transactionSpiProvider =
                new TransactionStateMachineSPIProviderV4(boltGraphDatabaseManagementServiceSPI, channel, clock);
        var boltSPI = new StateMachineSPIImpl(logging, transactionSpiProvider, channel);

        return new StateMachineV40(boltSPI, channel, clock, defaultDatabaseResolver, transactionManager);
    }

    @Override
    public StructRegistry<RequestMessage> requestMessageRegistry(BoltConnection connection) {
        return StructRegistry.<RequestMessage>builder()
                .register(HelloMessageDecoder.getInstance()) // since 3.x
                .register(new RunMessageDecoder(bookmarksParser))
                .register(DiscardMessageDecoder.getInstance())
                .register(PullMessageDecoder.getInstance())
                .register(new BeginMessageDecoder(bookmarksParser))
                .register(CommitMessageDecoder.getInstance()) // since 3.x
                .register(RollbackMessageDecoder.getInstance()) // since 3.x
                .register(new ResetMessageDecoder()) // since 3.x
                .register(new GoodbyeMessageDecoder(connection))
                .build(); // since 3.x
    }

    @Override
    public StructRegistry<ResponseMessage> responseMessageRegistry(BoltConnection connection) {
        return StructRegistry.<ResponseMessage>builder()
                .register(FailureMessageEncoder.getInstance())
                .register(IgnoredMessageEncoder.getInstance())
                .register(SuccessMessageEncoder.getInstance())
                .build();
    }
}
