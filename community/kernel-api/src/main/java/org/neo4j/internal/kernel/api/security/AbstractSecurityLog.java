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

import java.util.Map;
import java.util.regex.Pattern;
import org.neo4j.gqlstatus.GqlStatus;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.log4j.Neo4jMapMessage;

public abstract class AbstractSecurityLog {
    protected InternalLog inner;

    protected AbstractSecurityLog(InternalLog inner) {
        this.inner = inner;
    }

    public void debug(String message) {
        if (inner.isDebugEnabled()) {
            inner.debug(new SecurityLogLine(message));
        }
    }

    public void debug(String format, Object... args) {
        if (inner.isDebugEnabled()) {
            inner.debug(new SecurityLogLine(format, args));
        }
    }

    public void debug(ContextInfo context, String message) {
        if (inner.isDebugEnabled()) {
            inner.debug(new SecurityLogLine(context, message, null));
        }
    }

    public void debug(ContextInfo context, String format, Object... args) {
        if (inner.isDebugEnabled()) {
            inner.debug(new SecurityLogLine(context, format, args, null));
        }
    }

    public void info(String message) {
        if (inner.isInfoEnabled()) {
            inner.info(new SecurityLogLine(message));
        }
    }

    public void info(String format, Object... args) {
        if (inner.isInfoEnabled()) {
            inner.info(new SecurityLogLine(format, args));
        }
    }

    public void info(ContextInfo context, String message) {
        if (inner.isInfoEnabled()) {
            inner.info(new SecurityLogLine(context, message, null));
        }
    }

    public void info(ContextInfo context, String format, Object... args) {
        if (inner.isInfoEnabled()) {
            inner.info(new SecurityLogLine(context, format, args, null));
        }
    }

    public void warn(String message) {
        if (inner.isWarnEnabled()) {
            inner.warn(new SecurityLogLine(message));
        }
    }

    public void warn(String format, Object... args) {
        if (inner.isWarnEnabled()) {
            inner.warn(new SecurityLogLine(format, args));
        }
    }

    public void warn(ContextInfo context, String message) {
        if (inner.isWarnEnabled()) {
            inner.warn(new SecurityLogLine(context, message, null));
        }
    }

    public void warn(ContextInfo context, String format, Object... args) {
        if (inner.isWarnEnabled()) {
            inner.warn(new SecurityLogLine(context, format, args, null));
        }
    }

    public void error(String message) {
        if (inner.isErrorEnabled()) {
            inner.error(new SecurityLogLine(message));
        }
    }

    public void error(String format, Object... args) {
        if (inner.isErrorEnabled()) {
            inner.error(new SecurityLogLine(format, args));
        }
    }

    public void error(String message, GqlStatus gqlStatus) {
        if (inner.isErrorEnabled()) {
            inner.error(new SecurityLogLine(null, null, null, null, message, null, gqlStatus));
        }
    }

    public void error(ContextInfo context, String message) {
        error(context, message, (GqlStatus) null);
    }

    public void error(ContextInfo context, String format, Object... args) {
        if (inner.isErrorEnabled()) {
            inner.error(new SecurityLogLine(context, format, args, null));
        }
    }

    public void error(ContextInfo context, String message, GqlStatus gqlStatus) {
        if (inner.isErrorEnabled()) {
            inner.error(new SecurityLogLine(context, message, gqlStatus));
        }
    }

    public boolean isDebugEnabled() {
        return inner.isDebugEnabled();
    }

    public boolean isInfoEnabled() {
        return inner.isInfoEnabled();
    }

    public boolean isWarnEnabled() {
        return inner.isWarnEnabled();
    }

    public boolean isErrorEnabled() {
        return inner.isErrorEnabled();
    }

    public record ContextInfo(
            ClientConnectionInfo connectionInfo, String database, String authenticatedUser, String executingUser) {
        public static ContextInfo from(ClientConnectionInfo connectionInfo) {
            return new ContextInfo(connectionInfo, null, null, null);
        }

        public static ContextInfo from(LoginContext context) {
            return from(context, null);
        }

        public static ContextInfo from(LoginContext context, String database) {
            return new ContextInfo(
                    context.connectionInfo(),
                    database,
                    context.subject().authenticatedUser(),
                    context.subject().executingUser());
        }

        public static ContextInfo from(SecurityContext context) {
            return new ContextInfo(
                    context.connectionInfo(),
                    context.database(),
                    context.subject().authenticatedUser(),
                    context.subject().executingUser());
        }
    }

    static class SecurityLogLine extends Neo4jMapMessage {
        private final String executingUser;
        private final String message;
        private final String authenticatedUser;
        private final GqlStatus gqlStatus;
        private static final Pattern NEWLINE_PATTERN = Pattern.compile("\\R+");

        SecurityLogLine(String message) {
            this(null, null, null, null, message, null, null);
        }

        SecurityLogLine(String format, Object[] args) {
            this(null, null, null, null, format, args, null);
        }

        SecurityLogLine(ContextInfo contextInfo, String message, GqlStatus gqlStatus) {
            this(contextInfo, message, null, gqlStatus);
        }

        SecurityLogLine(ContextInfo contextInfo, String format, Object[] args, GqlStatus gqlStatus) {
            this(
                    contextInfo.connectionInfo,
                    contextInfo.database,
                    contextInfo.executingUser,
                    contextInfo.authenticatedUser,
                    format,
                    args,
                    gqlStatus);
        }

        private SecurityLogLine(
                ClientConnectionInfo connectionInfo,
                String database,
                String executingUser,
                String authenticatedUser,
                String format,
                Object[] args,
                GqlStatus gqlStatus) {
            super(7);
            String sourceString = connectionInfo != null ? connectionInfo.asConnectionDetails() : "";
            this.executingUser = executingUser;
            this.message = NEWLINE_PATTERN.matcher(formatMessage(format, args)).replaceAll(" ");
            this.authenticatedUser = authenticatedUser;
            this.gqlStatus = gqlStatus;

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
            if (gqlStatus != null) {
                with("errorInfo", Map.of("GQLSTATUS", gqlStatus.gqlStatusString()));
            }
            with("message", this.message);
        }

        private String formatMessage(String format, Object[] args) {
            return args == null || args.length == 0 ? format : String.format(format, args);
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
            if (gqlStatus != null) {
                sb.append(String.format("Exception thrown, %s: ", gqlStatus.gqlStatusString()));
            }
            sb.append(message);
        }
    }
}
