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
package org.neo4j.dbms.systemgraph;

import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_LABEL;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_NAME_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_UUID_PROPERTY;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.neo4j.cypher.internal.CypherVersion;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.MemoryLimitExceededException;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;

public class CommunityDefaultQueryLanguageLookup implements DefaultQueryLanguageLookup {
    private static String DATABASE_DEFAULT_LANGUAGE_PROPERTY = "defaultLanguage";

    private final SystemDatabaseProvider systemDb;
    private final JobScheduler scheduler;
    private final Lifecycle life = new Life();
    private final TransactionEventListener<Void> txListener = new TxListener();
    private final InternalLog log;

    private volatile boolean running = false;
    private volatile Map<NamedDatabaseId, CypherVersion> defaultQueryLanguageSnapshot = Map.of(); // Immutable

    public CommunityDefaultQueryLanguageLookup(
            SystemDatabaseProvider systemDatabaseProvider, JobScheduler jobScheduler, InternalLogProvider logProvider) {
        this.systemDb = systemDatabaseProvider;
        this.scheduler = jobScheduler;
        this.log = logProvider.getLog(getClass());
    }

    @Override
    public Optional<CypherVersion> defaultLanguage(NamedDatabaseId dbId) {
        return dbId != null ? Optional.ofNullable(defaultQueryLanguageSnapshot.get(dbId)) : Optional.empty();
    }

    public Lifecycle life() {
        return life;
    }

    public TransactionEventListener<Void> transactionListener() {
        return txListener;
    }

    private void tryUpdate() {
        int failureCount = 0;
        while (running) {
            try {
                final var snapshot = systemDb.query((tx) -> tx.findNodes(DATABASE_LABEL).stream()
                        .flatMap(n -> makeCypherVersion(n).stream().map(v -> Map.entry(makeDatabaseId(n), v)))
                        .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue)));
                this.defaultQueryLanguageSnapshot = snapshot;
                log.debug("Default query language snapshot update");
                return;
            } catch (SystemDatabaseProvider.SystemDatabasePanickedException e) {
                running = false; // If the system database has panicked, it's never coming back.
                log.warn("Default query language update failed because of system db panic", e);
                return;
            } catch (TransactionFailureException
                    | MemoryLimitExceededException
                    | SystemDatabaseProvider.SystemDatabaseUnavailableException e) {
                if (failureCount++ % 10 == 0) {
                    log.debug("Default query language update failed (attempt %s)".formatted(failureCount), e);
                }
                if (failureCount >= 100) {
                    log.error("Aborting default query language update after %s attempts".formatted(failureCount), e);
                    return;
                }
                sleep(100);
            } catch (Exception e) {
                log.error("Failed to read default query language", e);
                return;
            }
        }
    }

    private static NamedDatabaseId makeDatabaseId(Node databaseNode) {
        var name = (String) databaseNode.getProperty(DATABASE_NAME_PROPERTY);
        var uuid = UUID.fromString((String) databaseNode.getProperty(DATABASE_UUID_PROPERTY));
        return DatabaseIdFactory.from(name, uuid);
    }

    private static Optional<CypherVersion> makeCypherVersion(Node databaseNode) {
        return CypherVersion.fromStoredValueOptional(
                databaseNode.getProperty(DATABASE_DEFAULT_LANGUAGE_PROPERTY, null));
    }

    private static void sleep(int time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private class TxListener implements TransactionEventListener<Void> {
        @Override
        public Void beforeCommit(TransactionData data, Transaction transaction, GraphDatabaseService databaseService)
                throws Exception {
            return null;
        }

        @Override
        public void afterCommit(TransactionData data, Void state, GraphDatabaseService databaseService) {
            if (running)
                scheduler.schedule(
                        Group.TOPOLOGY_GRAPH_DBMS_MODEL, CommunityDefaultQueryLanguageLookup.this::tryUpdate);
        }

        @Override
        public void afterRollback(TransactionData data, Void state, GraphDatabaseService databaseService) {}

        @Override
        public Set<TransactionData.DataSelection> transactionDataSelection() {
            return Collections.emptySet();
        }
    }

    private class Life implements Lifecycle {
        @Override
        public void init() {}

        @Override
        public void start() throws Exception {
            scheduler
                    .schedule(Group.TOPOLOGY_GRAPH_DBMS_MODEL, () -> {
                        running = true;
                        tryUpdate();
                    })
                    .get();
        }

        @Override
        public void stop() {
            running = false;
        }

        @Override
        public void shutdown() {}
    }
}
