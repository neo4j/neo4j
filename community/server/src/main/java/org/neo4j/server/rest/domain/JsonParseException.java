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
package org.neo4j.server.rest.domain;

import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorMessageHolder;
import org.neo4j.gqlstatus.HasGqlStatusInfo;
import org.neo4j.kernel.api.exceptions.Status;

@SuppressWarnings("serial")
public class JsonParseException extends Exception implements Status.HasStatus, HasGqlStatusInfo {
    private final ErrorGqlStatusObject gqlStatusObject;
    private final String oldMessage;

    public JsonParseException(String message, Throwable cause) {
        super(message, cause);

        this.gqlStatusObject = null;
        this.oldMessage = message;
    }

    public JsonParseException(ErrorGqlStatusObject gqlStatusObject, String message, Throwable cause) {
        super(ErrorMessageHolder.getMessage(gqlStatusObject, message), cause);
        this.gqlStatusObject = gqlStatusObject;
        this.oldMessage = message;
    }

    public JsonParseException(Throwable cause) {
        super(cause);

        this.gqlStatusObject = null;
        this.oldMessage = HasGqlStatusInfo.getOldCauseMessage(cause);
    }

    public JsonParseException(ErrorGqlStatusObject gqlStatusObject, Throwable cause) {
        super(ErrorMessageHolder.getMessage(gqlStatusObject, HasGqlStatusInfo.getOldCauseMessage(cause)), cause);
        this.gqlStatusObject = gqlStatusObject;
        this.oldMessage = HasGqlStatusInfo.getOldCauseMessage(cause);
    }

    @Override
    public String getOldMessage() {
        return oldMessage;
    }

    @Override
    public Status status() {
        return Status.Request.InvalidFormat;
    }

    @Override
    public ErrorGqlStatusObject gqlStatusObject() {
        return gqlStatusObject;
    }
}
