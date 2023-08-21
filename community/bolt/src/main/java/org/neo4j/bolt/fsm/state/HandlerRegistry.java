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
package org.neo4j.bolt.fsm.state;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

final class HandlerRegistry<I, H> implements Iterable<H> {
    private final Map<Class<? extends I>, H> handlerMap;
    private final Map<Class<? extends I>, H> handlerCache = new ConcurrentHashMap<>();

    private HandlerRegistry(Map<Class<? extends I>, H> handlerMap) {
        this.handlerMap = new HashMap<>(handlerMap);
    }

    public static <I, H> Factory<I, H> builder() {
        return new Factory<I, H>();
    }

    public H find(Class<? extends I> type) {
        var cached = this.handlerCache.get(type);
        if (cached != null) {
            return cached;
        }

        var exactCandidates = this.handlerMap.get(type);
        if (exactCandidates != null) {
            this.handlerCache.put(type, exactCandidates);
            return exactCandidates;
        }

        var candidateKey = this.handlerMap.keySet().stream()
                .filter(candidate -> candidate.isAssignableFrom(type))
                .sorted((a, b) -> {
                    if (a.isAssignableFrom(b)) {
                        return 1;
                    }
                    if (b.isAssignableFrom(a)) {
                        return -1;
                    }

                    return 0;
                })
                .findFirst()
                .orElse(null);

        if (candidateKey == null) {
            return null;
        }

        var candidate = this.handlerMap.get(candidateKey);
        this.handlerCache.put(candidateKey, candidate);
        return candidate;
    }

    @Override
    public Iterator<H> iterator() {
        return this.handlerMap.values().iterator();
    }

    static final class Factory<I, H> {
        private final Map<Class<? extends I>, H> handlerMap = new HashMap<>();

        private Factory() {}

        public HandlerRegistry<I, H> build() {
            return new HandlerRegistry<>(this.handlerMap);
        }

        public void register(Class<? extends I> type, H handler) {
            this.handlerMap.put(type, handler);
        }
    }
}
