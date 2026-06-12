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
package org.neo4j.genai.util.monitor;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.genai.ai.image.embed.ImageVectorEmbeddingCallCountersMonitor;
import org.neo4j.genai.ai.text.completion.TextCompletionCallCountersMonitor;
import org.neo4j.genai.ai.text.embed.VectorEmbeddingCallCountersMonitor;
import org.neo4j.genai.ai.text.tokenCount.TextTokenCallCountersMonitor;
import org.neo4j.genai.util.GenAIMonitor;
import org.neo4j.genai.vector.DeprecatedVectorEncodingCallCountersMonitor;
import org.neo4j.graphdb.event.DatabaseEventContext;
import org.neo4j.graphdb.event.DatabaseEventListenerAdapter;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.DatabaseEventListeners;
import org.neo4j.util.VisibleForTesting;

/**
 * A utility class holding monitors to communicate with the metric system if that's present (enterprise edition only)
 */
public interface Monitors {
    <T extends GenAIMonitor> T ofType(Class<T> type);

    default DeprecatedVectorEncodingCallCountersMonitor deprecatedVectorEnc() {
        return ofType(DeprecatedVectorEncodingCallCountersMonitor.class);
    }

    default VectorEmbeddingCallCountersMonitor vectorEnc() {
        return ofType(VectorEmbeddingCallCountersMonitor.class);
    }

    default ImageVectorEmbeddingCallCountersMonitor imageVectorEnc() {
        return ofType(ImageVectorEmbeddingCallCountersMonitor.class);
    }

    default TextCompletionCallCountersMonitor textCompletion() {
        return ofType(TextCompletionCallCountersMonitor.class);
    }

    default TextTokenCallCountersMonitor textToken() {
        return ofType(TextTokenCallCountersMonitor.class);
    }

    @ServiceProvider
    class MonitorExtension extends ExtensionFactory<MonitorExtension.Dependencies> {

        public MonitorExtension() {
            super(ExtensionType.GLOBAL, "gen-ai-plugin-monitors");
        }

        @Override
        public Lifecycle newInstance(ExtensionContext context, Dependencies dependencies) {

            return new LifecycleAdapter() {

                @Override
                public void init() {
                    final var monitorCache = new MonitorCache(); // Shared globally
                    dependencies
                            .procedures()
                            .registerComponent(Monitors.class, (ctx) -> new CachingMonitors(ctx, monitorCache), true);

                    dependencies
                            .databaseEventListeners()
                            .registerDatabaseEventListener(new DatabaseEventListenerAdapter() {
                                @Override
                                public void databaseShutdown(DatabaseEventContext eventContext) {
                                    monitorCache.removeMonitors(eventContext.getDatabaseName());
                                }

                                @Override
                                public void databasePanic(DatabaseEventContext eventContext) {
                                    monitorCache.removeMonitors(eventContext.getDatabaseName());
                                }
                            });
                }
            };
        }

        public interface Dependencies {
            GlobalProcedures procedures();

            DatabaseEventListeners databaseEventListeners();
        }
    }

    @VisibleForTesting
    interface GlobalCache {
        Map<?, ?> snapshot();
    }
}

record CachingMonitors(Context ctx, MonitorCache cache) implements Monitors {

    @Override
    public <T extends GenAIMonitor> T ofType(Class<T> type) {
        return cache.getIfAbsentPut(
                ctx.graphDatabaseAPI().databaseName(),
                type,
                () -> ctx.dependencyResolver()
                        .resolveDependency(org.neo4j.monitoring.Monitors.class)
                        .newMonitor(type));
    }
}

// To avoid creating a new Proxy on each method call in a database
class MonitorCache implements Monitors.GlobalCache {
    private final Map<CacheKey, Object> monitors = new ConcurrentHashMap<>();

    public <T extends GenAIMonitor> T getIfAbsentPut(String dbName, Class<T> type, Supplier<T> monitorSupplier) {
        //noinspection unchecked
        return (T) monitors.computeIfAbsent(CacheKey.of(dbName, type), key -> monitorSupplier.get());
    }

    public void removeMonitors(String dbName) {
        monitors.keySet().removeIf(key -> Objects.equals(key.databaseName(), dbName));
    }

    @Override
    public Map<?, ?> snapshot() {
        return Map.copyOf(monitors);
    }

    record CacheKey(String databaseName, String type) {
        static CacheKey of(String databaseName, Class<?> type) {
            return new CacheKey(databaseName, type.getCanonicalName());
        }
    }
}
