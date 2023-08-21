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
package org.neo4j.internal.logging;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicInteger;
import org.neo4j.annotations.documented.DocumentedUtils;
import org.neo4j.annotations.documented.Warning;
import org.neo4j.logging.InternalLog;

public class LoggingReporterFactoryInvocationHandler implements InvocationHandler {
    private final InternalLog log;
    private final boolean formattedMessages;
    private final AtomicInteger errors = new AtomicInteger();
    private final AtomicInteger warnings = new AtomicInteger();

    public LoggingReporterFactoryInvocationHandler(InternalLog log, boolean formattedMessages) {
        this.log = log;
        this.formattedMessages = formattedMessages;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String message = formattedMessages
                ? DocumentedUtils.extractFormattedMessage(method, args)
                : DocumentedUtils.extractMessage(method);
        if (method.getAnnotation(Warning.class) == null) {
            errors.incrementAndGet();
            log.error(message);
        } else {
            warnings.incrementAndGet();
            log.warn(message);
        }
        return null;
    }

    public int errors() {
        return errors.get();
    }

    public int warnings() {
        return warnings.get();
    }
}
