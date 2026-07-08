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
package org.neo4j.batchimport.api;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.IntFunction;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.neo4j.batchimport.api.DetailedProgressReport.Stats;
import org.neo4j.batchimport.api.input.ApplicationMode;
import org.neo4j.common.EntityType;
import org.neo4j.common.FallbackTokenNameLookup;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.schema.SchemaUserDescription;

/**
 * Class for gathering information while import is running, into mutable state rich enough to be able to
 * generate {@link DetailedProgressReport} from.
 */
public class DetailedProgressReportBase {
    private final MutableStats nodeStats = new MutableStats();
    private final MutableStats relationshipStats = new MutableStats();
    private final PerTokenMutableStats nodePerLabelStats = new PerTokenMutableStats();
    private final PerTokenMutableStats relationshipPerTypeStats = new PerTokenMutableStats();
    private final MutableStats nodeIndexStats = new MutableStats();
    private final MutableStats nodeConstraintStats = new MutableStats();
    private final PerTokenMutableStats nodeIndexPerLabelStats = new PerTokenMutableStats();
    private final PerTokenMutableStats nodeConstraintPerLabelStats = new PerTokenMutableStats();
    private final MutableStats relationshipIndexStats = new MutableStats();
    private final MutableStats relationshipConstraintStats = new MutableStats();
    private final PerTokenMutableStats relationshipIndexPerTypeStats = new PerTokenMutableStats();
    private final PerTokenMutableStats relationshipConstraintPerTypeStats = new PerTokenMutableStats();
    private final Timer schemaTimer = new Timer();
    private final Timer nodeTimer = new Timer();
    private final Timer relationshipTimer = new Timer();
    private final long estimatedTotalNumberOfNodes;
    private final long estimatedTotalNumberOfRelationships;
    private final boolean trackPerEntityTokenStats;
    private volatile TokenNameLookup tokenNameLookup = SchemaUserDescription.TOKEN_ID_NAME_LOOKUP;

    /**
     * @param trackPerEntityTokenStats register stats per entity tokens too. Optional because it's
     * slightly more expensive and those stats are not always used.
     */
    public DetailedProgressReportBase(
            long estimatedTotalNumberOfNodes,
            long estimatedTotalNumberOfRelationships,
            boolean trackPerEntityTokenStats) {
        this.estimatedTotalNumberOfNodes = estimatedTotalNumberOfNodes;
        this.estimatedTotalNumberOfRelationships = estimatedTotalNumberOfRelationships;
        this.trackPerEntityTokenStats = trackPerEntityTokenStats;
    }

    public void setTokenNameLookup(TokenNameLookup tokenNameLookup) {
        // If we try to look up a token that does not exist yet, we default to the uninformative name
        // instead of failing with a null json serialization error.
        this.tokenNameLookup = new FallbackTokenNameLookup(tokenNameLookup, SchemaUserDescription.TOKEN_ID_NAME_LOOKUP);
    }

    public MutableStats nodeStats() {
        return nodeStats;
    }

    public MutableStats relationshipStats() {
        return relationshipStats;
    }

    public MutableStats indexStats(EntityType entityType) {
        return switch (entityType) {
            case NODE -> nodeIndexStats;
            case RELATIONSHIP -> relationshipIndexStats;
        };
    }

    public MutableStats constraintStats(EntityType entityType) {
        return switch (entityType) {
            case NODE -> nodeConstraintStats;
            case RELATIONSHIP -> relationshipConstraintStats;
        };
    }

    public Timer schemaTimer() {
        return schemaTimer;
    }

    public Timer nodeTimer() {
        return nodeTimer;
    }

    public Timer relationshipTimer() {
        return relationshipTimer;
    }

    public void registerNodeStats(ApplicationMode applicationMode, IntSet... entityTokens) {
        nodeStats.register(applicationMode);
        if (trackPerEntityTokenStats) {
            nodePerLabelStats.register(applicationMode, entityTokens);
        }
    }

    public void registerRelationshipStats(ApplicationMode applicationMode, int relationshipType) {
        relationshipStats.register(applicationMode);
        if (trackPerEntityTokenStats) {
            relationshipPerTypeStats.register(applicationMode, relationshipType);
        }
    }

    private PerTokenMutableStats splitIndexStats(EntityType entityType) {
        return switch (entityType) {
            case NODE -> nodeIndexPerLabelStats;
            case RELATIONSHIP -> relationshipIndexPerTypeStats;
        };
    }

    private PerTokenMutableStats splitConstraintStats(EntityType entityType) {
        return switch (entityType) {
            case NODE -> nodeConstraintPerLabelStats;
            case RELATIONSHIP -> relationshipConstraintPerTypeStats;
        };
    }

    public void registerIndexStats(ApplicationMode applicationMode, EntityType entityType, IntSet... entityTokens) {
        indexStats(entityType).register(applicationMode);
        if (trackPerEntityTokenStats) {
            splitIndexStats(entityType).register(applicationMode, entityTokens);
        }
    }

    public void registerConstraintStats(
            ApplicationMode applicationMode, EntityType entityType, IntSet... entityTokens) {
        constraintStats(entityType).register(applicationMode);
        if (trackPerEntityTokenStats) {
            splitConstraintStats(entityType).register(applicationMode, entityTokens);
        }
    }

    public DetailedProgressReport snapshot() {
        return new DetailedProgressReport(
                estimatedTotalNumberOfNodes,
                estimatedTotalNumberOfRelationships,
                nodeStats.snapshot(),
                relationshipStats.snapshot(),
                nodePerLabelStats.snapshot(tokenNameLookup::labelGetName),
                relationshipPerTypeStats.snapshot(tokenNameLookup::relationshipTypeGetName),
                nodeIndexStats.snapshot(),
                nodeConstraintStats.snapshot(),
                nodeIndexPerLabelStats.snapshot(tokenNameLookup::labelGetName),
                nodeConstraintPerLabelStats.snapshot(tokenNameLookup::labelGetName),
                relationshipIndexStats.snapshot(),
                relationshipConstraintStats.snapshot(),
                relationshipIndexPerTypeStats.snapshot(tokenNameLookup::relationshipTypeGetName),
                relationshipConstraintPerTypeStats.snapshot(tokenNameLookup::relationshipTypeGetName),
                nodeTimer.snapshot(),
                relationshipTimer.snapshot(),
                schemaTimer.snapshot());
    }

    public record MutableStats(
            LongAdder numProcessed, LongAdder numCreated, LongAdder numUpdated, LongAdder numDeleted) {
        public MutableStats() {
            this(new LongAdder(), new LongAdder(), new LongAdder(), new LongAdder());
        }

        public Stats snapshot() {
            return new Stats(
                    numProcessed.longValue(), numCreated.longValue(), numUpdated.longValue(), numDeleted.longValue());
        }

        /**
         * Convenience method to register a creation/update/deletion, based on {@link ApplicationMode}.
         */
        public void register(ApplicationMode applicationMode) {
            switch (applicationMode) {
                case CREATE -> numCreated.increment();
                case UPDATE -> numUpdated.increment();
                case DELETE -> numDeleted.increment();
            }
        }

        /**
         * Decrements the creation count. Used to factor duplicates out of the statistics.
         */
        public void unregister() {
            unregister(1L);
        }

        /**
         * Decrements the creation count. Used to factor duplicates out of the statistics.
         *
         * @param amount the number of created entities to unregister.
         */
        public void unregister(long amount) {
            numCreated.add(amount * -1L);
        }
    }

    public static class Timer {
        private volatile long start;
        private volatile long end;

        public void start() {
            this.start = System.nanoTime();
        }

        public void end() {
            this.end = System.nanoTime();
        }

        public Duration snapshot() {
            long start = this.start;
            if (start == 0) {
                return Duration.ZERO;
            }

            long end = this.end;
            if (end == 0) {
                end = System.nanoTime();
            }
            return Duration.ofNanos(end - start);
        }
    }

    public static class PerTokenMutableStats {
        // Have a simple final array for the (presumably) most common token IDs, for performance
        private final MutableStats[] lowIdStats = new MutableStats[100];
        private final Map<Integer, MutableStats> higherIdStats = new ConcurrentHashMap<>();

        PerTokenMutableStats() {
            for (int tokenId = 0; tokenId < lowIdStats.length; tokenId++) {
                lowIdStats[tokenId] = new MutableStats();
            }
        }

        /**
         * Convenience method to register creation/update/deletion of an entity, with additional details
         * about its entity tokens. There could be multiple sets of entity tokens, e.g. added, removed a.s.o.
         */
        private void register(ApplicationMode applicationMode, IntSet... entityTokens) {
            for (var tokens : entityTokens) {
                tokens.forEach(tokenId -> {
                    var stats = forToken(tokenId);
                    stats.numProcessed.increment();
                    stats.register(applicationMode);
                });
            }
        }

        private void register(ApplicationMode applicationMode, int entityTokenId) {
            var stats = forToken(entityTokenId);
            stats.numProcessed.increment();
            stats.register(applicationMode);
        }

        private Map<String, Stats> snapshot(IntFunction<String> tokenNameLookup) {
            Map<String, Stats> snapshot = new HashMap<>();
            for (int tokenId = 0; tokenId < lowIdStats.length; tokenId++) {
                var stats = lowIdStats[tokenId];
                if (stats.numProcessed.sum() > 0) {
                    snapshot.put(tokenNameLookup.apply(tokenId), stats.snapshot());
                }
            }
            higherIdStats.forEach(
                    (tokenId, mutableStats) -> snapshot.put(tokenNameLookup.apply(tokenId), mutableStats.snapshot()));
            return snapshot;
        }

        private MutableStats forToken(int tokenId) {
            return tokenId < lowIdStats.length
                    ? lowIdStats[tokenId]
                    : higherIdStats.computeIfAbsent(tokenId, k -> new MutableStats());
        }
    }
}
