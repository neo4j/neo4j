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
import org.neo4j.packstream.io.Type;
import org.neo4j.packstream.io.TypeMarker;

public class UnexpectedTypeException extends PackstreamReaderException implements Status.HasStatus, HasGqlStatusInfo {
    private final Type expected;
    private final Type actual;
    private final ErrorGqlStatusObject gqlStatusObject;
    private final String oldMessage;

    public UnexpectedTypeException(Type expected, Type actual) {
        super("Unexpected type: Expected " + expected + " but got " + actual);
        this.expected = expected;
        this.actual = actual;

        this.gqlStatusObject = null;
        this.oldMessage = "Unexpected type: Expected " + expected + " but got " + actual;
    }

    public UnexpectedTypeException(ErrorGqlStatusObject gqlStatusObject, Type expected, Type actual) {
        super(ErrorMessageHolder.getMessage(
                gqlStatusObject, "Unexpected type: Expected " + expected + " but got " + actual));
        this.gqlStatusObject = gqlStatusObject;

        this.expected = expected;
        this.actual = actual;
        this.oldMessage = "Unexpected type: Expected " + expected + " but got " + actual;
    }

    public UnexpectedTypeException(Type actual) {
        super("Unexpected type: " + actual);
        this.expected = null;
        this.actual = actual;

        this.gqlStatusObject = null;
        this.oldMessage = "Unexpected type: " + actual;
    }

    public UnexpectedTypeException(ErrorGqlStatusObject gqlStatusObject, Type actual) {
        super(ErrorMessageHolder.getMessage(gqlStatusObject, "Unexpected type: " + actual));
        this.gqlStatusObject = gqlStatusObject;

        this.expected = null;
        this.actual = actual;
        this.oldMessage = "Unexpected type: " + actual;
    }

    public UnexpectedTypeException(Type expected, TypeMarker actual) {
        this(expected, actual.getType());
    }

    public UnexpectedTypeException(ErrorGqlStatusObject gqlStatusObject, Type expected, TypeMarker actual) {
        this(gqlStatusObject, expected, actual.getType());
    }

    protected UnexpectedTypeException(String message, Type expected, Type actual) {
        super(message);
        this.expected = expected;
        this.actual = actual;

        this.gqlStatusObject = null;
        this.oldMessage = message;
    }

    protected UnexpectedTypeException(
            ErrorGqlStatusObject gqlStatusObject, String message, Type expected, Type actual) {
        super(ErrorMessageHolder.getMessage(gqlStatusObject, message));
        this.gqlStatusObject = gqlStatusObject;

        this.expected = expected;
        this.actual = actual;
        this.oldMessage = message;
    }

    public Type getExpected() {
        return this.expected;
    }

    public Type getActual() {
        return this.actual;
    }

    @Override
    public String getOldMessage() {
        return this.oldMessage;
    }

    @Override
    public Status status() {
        // Technically a protocol violation in most cases but some drivers in dynamically typed languages will simply
        // pass
        // on whatever the caller originally passed without validation ...
        return Status.Request.Invalid;
    }

    @Override
    public ErrorGqlStatusObject gqlStatusObject() {
        return gqlStatusObject;
    }
}
