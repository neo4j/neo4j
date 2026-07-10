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
package org.neo4j.kernel.api.exceptions;

import static java.lang.System.lineSeparator;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;

/**
 * An exception which can have context added to it throughout the exception handling process. It can print its message
 * and all provided context in a well-formatted error, which is intended to supplant the usual big angry wall of
 * stacktraces, making the CLI output easier for users to read.
 */
public abstract class ConsoleFriendlyException extends RuntimeException {

    static final String DIVIDER = "-----------------------------------------------------";

    private final boolean prettyPrint;
    private final List<String> supplementaryMessages = new ArrayList<>();

    /**
     * Creates a new {@link ConsoleFriendlyException}.
     *
     * @param message the primary detail message.
     */
    public ConsoleFriendlyException(String message) {
        this(message, true);
    }

    /**
     * Creates a new {@link ConsoleFriendlyException}.
     *
     * @param message the primary detail message.
     * @param prettyPrint whether the pretty printing functionality should be used (it may not be appropriate if helpful
     *                    output cannot be guaranteed).
     */
    public ConsoleFriendlyException(String message, boolean prettyPrint) {
        super(message);
        this.prettyPrint = prettyPrint;
    }

    /**
     * Creates a new {@link ConsoleFriendlyException}.
     *
     * @param cause the throwable that caused this if known (otherwise {@code null}).
     */
    public ConsoleFriendlyException(Throwable cause) {
        this(cause, true);
    }

    /**
     * Creates a new {@link ConsoleFriendlyException}.
     *
     * @param cause the throwable that caused this if known (otherwise {@code null}).
     * @param prettyPrint whether the pretty printing functionality should be used (it may not be appropriate if helpful
     *                    output cannot be guaranteed).
     */
    public ConsoleFriendlyException(Throwable cause, boolean prettyPrint) {
        super(cause.getMessage(), cause);
        this.prettyPrint = prettyPrint;
        populateSupplementaryMessages(cause);
    }

    /**
     * Creates a new {@link ConsoleFriendlyException}.
     *
     * @param message the primary detail message.
     * @param cause the throwable that caused this if known (otherwise {@code null}).
     */
    public ConsoleFriendlyException(String message, Throwable cause) {
        this(message, cause, true);
    }

    /**
     * Creates a new {@link ConsoleFriendlyException}.
     *
     * @param message the primary detail message.
     * @param cause the throwable that caused this if known (otherwise {@code null}).
     * @param prettyPrint whether the pretty printing functionality should be used (it may not be appropriate if helpful
     *                    output cannot be guaranteed).
     */
    public ConsoleFriendlyException(String message, Throwable cause, boolean prettyPrint) {
        super(message, cause);
        this.prettyPrint = prettyPrint;
        populateSupplementaryMessages(cause);
    }

    /**
     * Check whether it is appropriate to pretty print the contents of this error. As a general rule, pretty printing
     * (especially in lieu of verbose exception output) will only be considered appropriate if helpful messages can be
     * guaranteed.
     *
     * @return {@code true} if this will pretty print.
     */
    public boolean willPrettyPrint() {
        return prettyPrint;
    }

    private void populateSupplementaryMessages(Throwable cause) {
        if (cause != null) {
            addSupplementaryMessage(cause.getMessage());
        }
    }

    /**
     * Adds an extra message. These extra messages will be printed in a group underneath the main error message if this
     * is configured to pretty print.
     *
     * @param message the extra message.
     */
    public ConsoleFriendlyException addSupplementaryMessage(String message) {
        supplementaryMessages.add(message);
        return this;
    }

    @Override
    public String getMessage() {
        var displayCauseMessage = true;
        var message = super.getMessage();
        var sb = new StringBuilder();
        Set<String> seenSupplementaryMessages = new HashSet<>();
        for (var msg : supplementaryMessages) {
            if (!seenSupplementaryMessages.add(msg)) {
                continue;
            }

            if (Objects.equals(message, msg)) {
                displayCauseMessage = false;
            }
            sb.append(lineSeparator()).append(msg).append(lineSeparator());
        }

        if (displayCauseMessage) {
            sb.insert(0, lineSeparator()).insert(0, message);
        }
        return sb.toString();
    }

    /**
     * Formats the exception's message, along with any supplementary messages, in a neatly formatted way. Intended to be
     * called manually towards the end of the exception-handling process, as an alternative to spamming the user with a
     * big angry wall of stacktraces.
     *
     * @param out the {@link PrintStream} to send the error message to.
     */
    public void prettyPrint(PrintStream out) {
        prettyPrint(out, null);
    }

    /**
     * Formats the exception's message, along with any supplementary messages, in a neatly formatted way. Intended to be
     * called manually towards the end of the exception-handling process, as an alternative to spamming the user with a
     * big angry wall of stacktraces.
     *
     * @param out the {@link PrintStream} to send the error message to.
     * @param headline a String to insert within the prettyPrint output, above the error message. Consider using it to
     *                 explain the end result of what happened (e.g. "Command Failed").
     */
    public void prettyPrint(PrintStream out, String headline) {
        out.println(DIVIDER);
        if (!StringUtils.isEmpty(headline)) {
            out.println(headline);
        }
        out.print(getMessage());
        out.println(DIVIDER);
    }

    /**
     * Utility method for checking whether it is appropriate for the given throwable to use
     * {@link ConsoleFriendlyException}'s pretty printing functionality. As a general rule, pretty printing (especially
     * in lieu of verbose exception output) will only be considered appropriate if helpful messages can be guaranteed.
     *
     * @param throwable the throwable.
     * @return {@code true} if throwable is a {@link ConsoleFriendlyException} that has been designated to pretty print.
     */
    public static boolean willPrettyPrint(Throwable throwable) {
        if (throwable instanceof ConsoleFriendlyException consoleFriendlyException) {
            return consoleFriendlyException.willPrettyPrint();
        }
        return false;
    }
}
