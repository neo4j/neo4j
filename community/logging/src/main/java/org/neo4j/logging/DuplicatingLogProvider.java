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
package org.neo4j.logging;

import java.util.List;
import org.neo4j.io.IOUtils;
import org.neo4j.logging.log4j.LoggerTarget;
import org.neo4j.util.VisibleForTesting;

/**
 * A {@link InternalLogProvider} implementation that duplicates all messages to other LogProvider instances
 */
public class DuplicatingLogProvider implements InternalLogProvider {
    private final InternalLogProvider logProvider1;
    private final InternalLogProvider logProvider2;

    public DuplicatingLogProvider(InternalLogProvider logProvider1, InternalLogProvider logProvider2) {
        this.logProvider1 = logProvider1;
        this.logProvider2 = logProvider2;
    }

    @Override
    public InternalLog getLog(Class<?> loggingClass) {
        return new DuplicatingLog(logProvider1.getLog(loggingClass), logProvider2.getLog(loggingClass));
    }

    @Override
    public InternalLog getLog(String name) {
        return new DuplicatingLog(logProvider1.getLog(name), logProvider2.getLog(name));
    }

    @Override
    public InternalLog getLog(LoggerTarget target) {
        return new DuplicatingLog(logProvider1.getLog(target), logProvider2.getLog(target));
    }

    @Override
    public void close() {
        IOUtils.closeAllUnchecked(logProvider1, logProvider2);
    }

    @VisibleForTesting
    public List<InternalLogProvider> getTargetLogProviders() {
        return List.of(logProvider1, logProvider2);
    }
}
