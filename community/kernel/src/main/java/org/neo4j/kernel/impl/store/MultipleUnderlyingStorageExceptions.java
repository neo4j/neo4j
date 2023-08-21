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
package org.neo4j.kernel.impl.store;

import static java.lang.String.format;

import java.util.Collections;
import java.util.Set;
import org.neo4j.exceptions.UnderlyingStorageException;

public class MultipleUnderlyingStorageExceptions extends UnderlyingStorageException {
    public final Set<IndexFailureRecord> exceptions;

    public MultipleUnderlyingStorageExceptions(Set<IndexFailureRecord> exceptions) {
        super(buildMessage(exceptions));
        this.exceptions = Collections.unmodifiableSet(exceptions);

        for (IndexFailureRecord failureRecord : exceptions) {
            this.addSuppressed(failureRecord.exception());
        }
    }

    private static String buildMessage(Set<IndexFailureRecord> exceptions) {
        StringBuilder builder = new StringBuilder();
        builder.append("Errors when closing (flushing) index updaters:");

        for (IndexFailureRecord pair : exceptions) {
            builder.append(
                    format(" (%s) %s", pair.descriptor(), pair.exception().getMessage()));
        }

        return builder.toString();
    }
}
