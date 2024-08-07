/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.log.stresstest.workload;

import static java.lang.System.currentTimeMillis;
import static org.neo4j.common.Subject.ANONYMOUS;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.neo4j.kernel.impl.api.CompleteTransaction;
import org.neo4j.kernel.impl.api.TestCommand;
import org.neo4j.kernel.impl.api.txid.TransactionIdGenerator;
import org.neo4j.kernel.impl.transaction.log.CompleteCommandBatch;
import org.neo4j.storageengine.api.Commitment;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.LatestVersions;

class TransactionRepresentationFactory {
    private final CommandGenerator commandGenerator = new CommandGenerator();

    CompleteTransaction nextTransaction(long txId) {
        CompleteCommandBatch representation = new CompleteCommandBatch(
                createRandomCommands(),
                UNKNOWN_CONSENSUS_INDEX,
                currentTimeMillis(),
                txId,
                currentTimeMillis(),
                42,
                LatestVersions.LATEST_KERNEL_VERSION,
                ANONYMOUS);
        return new CompleteTransaction(
                representation,
                NULL_CONTEXT,
                StoreCursors.NULL,
                Commitment.NO_COMMITMENT,
                TransactionIdGenerator.EMPTY);
    }

    private List<StorageCommand> createRandomCommands() {
        int commandNum = ThreadLocalRandom.current().nextInt(1, 17);
        List<StorageCommand> commands = new ArrayList<>(commandNum);
        for (int i = 0; i < commandNum; i++) {
            commands.add(commandGenerator.nextCommand());
        }
        return commands;
    }

    private static class CommandGenerator {
        private final ThreadLocalRandom random = ThreadLocalRandom.current();

        StorageCommand nextCommand() {
            int length = random.nextInt(100 + 1);
            byte[] bytes = new byte[length];
            random.nextBytes(bytes);
            return new TestCommand(bytes);
        }
    }
}
