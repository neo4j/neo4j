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
package org.neo4j.logging.log4j;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.lookup.StrLookup;

/**
 * Injects a context for lookups during the log4j configuration phase.
 */
public abstract class AbstractLookup implements StrLookup {
    private static final ThreadLocal<LookupContext> LOOKUP_CONTEXT = ThreadLocal.withInitial(() -> null);

    final LookupContext context;

    public static void setLookupContext(LookupContext context) {
        LOOKUP_CONTEXT.set(context);
    }

    public static void removeLookupContext() {
        LOOKUP_CONTEXT.remove();
    }

    AbstractLookup() {
        context = LOOKUP_CONTEXT.get();
    }

    @Override
    public String lookup(LogEvent event, String key) {
        return lookup(key);
    }
}
