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
package org.neo4j.gqlstatus;

import java.util.ArrayList;
import java.util.function.Function;

/** Formats messages according to very simple substitution rules. Every %s is considered a parameter.  */
public interface SimpleMessageFormat {

    String format(Object[] args);

    StringBuilder format(StringBuilder builder, Object[] args);

    int parameterCount(); // Visible for testing

    static SimpleMessageFormat compile(String template) {
        return compile(template, TO_STRING_FORMATTER);
    }

    static SimpleMessageFormat compile(String template, Function<Object, String> formatter) {
        final var offsets = new ArrayList<Integer>();
        final var message = new StringBuilder();
        final var substitution = "%s";
        int offset = 0, prevOffset = 0;
        while ((offset = template.indexOf(substitution, offset)) != -1) {
            message.append(template, prevOffset, offset);
            offsets.add(message.length());
            offset += substitution.length();
            prevOffset = offset;
        }

        if (offsets.isEmpty()) {
            return new StaticMessage(template);
        } else {
            if (prevOffset < template.length()) message.append(template, prevOffset, template.length());
            return new FormatParameters(
                    message.toString(), offsets.stream().mapToInt(o -> o).toArray(), formatter);
        }
    }

    Function<Object, String> TO_STRING_FORMATTER = new Function<Object, String>() {
        @Override
        public String apply(Object o) {
            return String.valueOf(o);
        }
    };
}

final class FormatParameters implements SimpleMessageFormat {
    private final String message;
    private final int[] offsets;
    private final Function<Object, String> formatter;

    FormatParameters(String message, int[] offsets, Function<Object, String> formatter) {
        assert isOffsetsInBounds(message, offsets);
        this.message = message;
        this.offsets = offsets;
        this.formatter = formatter;
    }

    private static boolean isOffsetsInBounds(String message, int[] offsets) {
        final var length = message.length();
        for (int i = 0; i < offsets.length; i++) {
            if (offsets[i] > length) return false;
            if (i > 0 && offsets[i] < offsets[i - 1]) return false;
        }
        return true;
    }

    @Override
    public String format(Object[] args) {
        final var estimatedParamSize = 24 * offsets.length; // We guess the size of the params.
        return format(new StringBuilder(message.length() + estimatedParamSize), args)
                .toString();
    }

    @Override
    public StringBuilder format(StringBuilder builder, Object[] args) {
        if (args == null) args = new Object[0];
        int lastOffset = 0;
        for (int i = 0; i < offsets.length; ++i) {
            int currentOffset = offsets[i];
            builder.append(message, lastOffset, currentOffset);
            builder.append(formatter.apply(i < args.length ? args[i] : null));
            lastOffset = currentOffset;
        }
        if (lastOffset < message.length()) builder.append(message, lastOffset, message.length());
        return builder;
    }

    // Visible for testing
    @Override
    public int parameterCount() {
        return offsets.length;
    }
}

final class StaticMessage implements SimpleMessageFormat {
    private final String message;

    StaticMessage(String message) {
        this.message = message;
    }

    @Override
    public String format(Object[] args) {
        return message;
    }

    @Override
    public StringBuilder format(StringBuilder builder, Object[] args) {
        return builder.append(message);
    }

    // Visible for testing
    @Override
    public int parameterCount() {
        return 0;
    }
}
