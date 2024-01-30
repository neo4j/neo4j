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
package org.neo4j.internal.kernel.api.security;

import static org.neo4j.internal.helpers.Strings.escape;

import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.log4j.Neo4jMapMessage;

public abstract class AbstractSecurityLog {
    protected InternalLog inner;

    protected AbstractSecurityLog(InternalLog inner) {
        this.inner = inner;
    }

    public void debug(String message) {
        inner.debug(new SecurityLogLine(message));
    }

    public void debug(SecurityContext context, String message) {
        AuthSubject subject = context.subject();
        inner.debug(new SecurityLogLine(
                context.connectionInfo(),
                context.database(),
                subject.executingUser(),
                message,
                subject.authenticatedUser()));
    }

    public void info(String message) {
        inner.info(new SecurityLogLine(message));
    }

    public void info(LoginContext context, String message) {
        AuthSubject subject = context.subject();
        inner.info(new SecurityLogLine(
                context.connectionInfo(), null, subject.executingUser(), message, subject.authenticatedUser()));
    }

    public void info(SecurityContext context, String message) {
        AuthSubject subject = context.subject();
        inner.info(new SecurityLogLine(
                context.connectionInfo(),
                context.database(),
                subject.executingUser(),
                message,
                subject.authenticatedUser()));
    }

    public void warn(String message) {
        inner.warn(new SecurityLogLine(message));
    }

    public void warn(SecurityContext context, String message) {
        AuthSubject subject = context.subject();
        inner.warn(new SecurityLogLine(
                context.connectionInfo(),
                context.database(),
                subject.executingUser(),
                message,
                subject.authenticatedUser()));
    }

    public void error(String message) {
        inner.error(new SecurityLogLine(message));
    }

    public void error(ClientConnectionInfo connectionInfo, String message) {
        inner.error(new SecurityLogLine(connectionInfo, null, null, message, null));
    }

    public void error(LoginContext context, String message) {
        AuthSubject subject = context.subject();
        inner.error(new SecurityLogLine(
                context.connectionInfo(), null, subject.executingUser(), message, subject.authenticatedUser()));
    }

    public void error(LoginContext context, String database, String message) {
        AuthSubject subject = context.subject();
        inner.error(new SecurityLogLine(
                context.connectionInfo(), database, subject.executingUser(), message, subject.authenticatedUser()));
    }

    public void error(SecurityContext context, String message) {
        AuthSubject subject = context.subject();
        inner.error(new SecurityLogLine(
                context.connectionInfo(),
                context.database(),
                subject.executingUser(),
                message,
                subject.authenticatedUser()));
    }

    public boolean isDebugEnabled() {
        return inner.isDebugEnabled();
    }

    static class SecurityLogLine extends Neo4jMapMessage {
        private final String executingUser;
        private final String message;
        private final String authenticatedUser;

        SecurityLogLine(String message) {
            this(null, null, null, message, null);
        }

        SecurityLogLine(
                ClientConnectionInfo connectionInfo,
                String database,
                String executingUser,
                String message,
                String authenticatedUser) {
            super(7);
            String sourceString = connectionInfo != null ? connectionInfo.asConnectionDetails() : "";
            this.executingUser = executingUser;
            // clean message of newlines
            this.message = message.replaceAll("\\R+", " ");
            this.authenticatedUser = authenticatedUser;

            with("type", "security");
            with("source", sourceString);
            if (database != null) {
                with("database", database);
            }
            if (executingUser != null && !executingUser.isEmpty()) {
                with("executingUser", executingUser);
            }
            if (authenticatedUser != null && !authenticatedUser.isEmpty()) {
                with("authenticatedUser", authenticatedUser);
            }
            with("message", this.message);
        }

        @Override
        protected void formatAsString(StringBuilder sb) {
            if (executingUser != null && !executingUser.isEmpty()) {
                if (executingUser.equals(authenticatedUser)) {
                    sb.append("[").append(escape(executingUser)).append("]: ");
                } else {
                    sb.append(String.format("[%s:%s]: ", escape(authenticatedUser), escape(executingUser)));
                }
            }
            sb.append(message);
        }
    }
}
