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
package org.neo4j.server.logging.slf4j;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.status.StatusLogger;
import org.slf4j.spi.MDCAdapter;

class SLF4JToLog4jMDCAdapter implements MDCAdapter {
    private static final Logger LOGGER = StatusLogger.getLogger();

    private final ThreadLocalMapOfStacks threadLocalMapOfDeques = new ThreadLocalMapOfStacks();

    @Override
    public void put(String key, String val) {
        ThreadContext.put(key, val);
    }

    @Override
    public String get(String key) {
        return ThreadContext.get(key);
    }

    @Override
    public void remove(String key) {
        ThreadContext.remove(key);
    }

    @Override
    public void clear() {
        ThreadContext.clearMap();
    }

    @Override
    public Map<String, String> getCopyOfContextMap() {
        return ThreadContext.getContext();
    }

    @Override
    public void setContextMap(Map<String, String> contextMap) {
        ThreadContext.clearMap();
        ThreadContext.putAll(contextMap);
    }

    @Override
    public void pushByKey(String key, String value) {
        if (key == null) {
            ThreadContext.push(value);
        } else {
            String oldValue = threadLocalMapOfDeques.peekByKey(key);
            if (!Objects.equals(ThreadContext.get(key), oldValue)) {
                LOGGER.warn("The key {} was used in both the string and stack-valued MDC.", key);
            }
            threadLocalMapOfDeques.pushByKey(key, value);
            ThreadContext.put(key, value);
        }
    }

    @Override
    public String popByKey(String key) {
        if (key == null) {
            return ThreadContext.getDepth() > 0 ? ThreadContext.pop() : null;
        }
        String value = threadLocalMapOfDeques.popByKey(key);
        if (!Objects.equals(ThreadContext.get(key), value)) {
            LOGGER.warn("The key {} was used in both the string and stack-valued MDC.", key);
        }
        ThreadContext.put(key, threadLocalMapOfDeques.peekByKey(key));
        return value;
    }

    @Override
    public Deque<String> getCopyOfDequeByKey(String key) {
        if (key == null) {
            ThreadContext.ContextStack stack = ThreadContext.getImmutableStack();
            Deque<String> copy = new ArrayDeque<>(stack.size());
            stack.forEach(copy::push);
            return copy;
        }
        return threadLocalMapOfDeques.getCopyOfDequeByKey(key);
    }

    @Override
    public void clearDequeByKey(String key) {
        if (key == null) {
            ThreadContext.clearStack();
        } else {
            threadLocalMapOfDeques.clearByKey(key);
            ThreadContext.put(key, null);
        }
    }

    private static class ThreadLocalMapOfStacks {
        private final ThreadLocal<Map<String, Deque<String>>> tlMapOfStacks = ThreadLocal.withInitial(HashMap::new);

        void pushByKey(String key, String value) {
            tlMapOfStacks
                    .get()
                    .computeIfAbsent(key, ignored -> new ArrayDeque<>())
                    .push(value);
        }

        String popByKey(String key) {
            Deque<String> deque = tlMapOfStacks.get().get(key);
            return deque != null ? deque.poll() : null;
        }

        Deque<String> getCopyOfDequeByKey(String key) {
            Deque<String> deque = tlMapOfStacks.get().get(key);
            return deque != null ? new ArrayDeque<>(deque) : null;
        }

        void clearByKey(String key) {
            Deque<String> deque = tlMapOfStacks.get().get(key);
            if (deque != null) {
                deque.clear();
            }
        }

        String peekByKey(String key) {
            Deque<String> deque = tlMapOfStacks.get().get(key);
            return deque != null ? deque.peek() : null;
        }
    }
}
