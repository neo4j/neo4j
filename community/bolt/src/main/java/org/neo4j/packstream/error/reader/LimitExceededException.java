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
package org.neo4j.packstream.error.reader;

import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorMessageHolder;
import org.neo4j.gqlstatus.HasGqlStatusInfo;
import org.neo4j.kernel.api.exceptions.Status;

public class LimitExceededException extends PackstreamReaderException implements Status.HasStatus, HasGqlStatusInfo {
    private final long limit;
    private final long actual;
    private final ErrorGqlStatusObject gqlStatusObject;
    private final String oldMessage;

    public LimitExceededException(long limit, long actual) {
        super("Value of size " + actual + " exceeded limit of " + limit);

        this.limit = limit;
        this.actual = actual;

        this.gqlStatusObject = null;
        this.oldMessage = "Value of size " + actual + " exceeded limit of " + limit;
    }

    public LimitExceededException(ErrorGqlStatusObject gqlStatusObject, long limit, long actual) {
        super(ErrorMessageHolder.getMessage(
                gqlStatusObject, "Value of size " + actual + " exceeded limit of " + limit));
        this.gqlStatusObject = gqlStatusObject;

        this.limit = limit;
        this.actual = actual;
        this.oldMessage = "Value of size " + actual + " exceeded limit of " + limit;
    }

    public long getLimit() {
        return this.limit;
    }

    public long getActual() {
        return this.actual;
    }

    @Override
    public String getOldMessage() {
        return oldMessage;
    }

    @Override
    public Status status() {
        return Status.Request.Invalid;
    }

    @Override
    public ErrorGqlStatusObject gqlStatusObject() {
        return gqlStatusObject;
    }
}
