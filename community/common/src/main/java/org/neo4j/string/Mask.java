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
package org.neo4j.string;

import java.util.function.Consumer;
import java.util.function.Supplier;
import org.eclipse.collections.api.block.procedure.Procedure;
import org.eclipse.collections.impl.block.procedure.AppendStringProcedure;

/**
 * Used to implement masking of user data fields when stringifying records, commands, and log entries.
 */
public interface Mask {
    /**
     * Returns the string representation of the argument or a placeholder if masking.
     *
     * @param value value to stringify or mask
     * @return potentially masked string representation
     */
    default String filter(Object value) {
        return lazyFilter(value::toString);
    }

    /**
     * Returns a string representation, in the format of a list, of an iterable where each element is masked.
     *
     * @param iterable {@link Iterable} iterable to mask
     */
    default String filter(Iterable<? extends Maskable> iterable) {
        final var sb = new StringBuilder();
        append(sb, iterable);
        return sb.toString();
    }

    /**
     * Returns the value produced by the supplier or a placeholder if masking.
     *
     * @param supplier supplier of a string to return or mask
     * @return potentially masked string representation
     */
    String lazyFilter(Supplier<String> supplier);

    /**
     * Calls the {@link Consumer} with the {@link StringBuilder} or writes a placeholder to it if masking.
     *
     * @param builder {@link StringBuilder} to write to
     * @param build   consumer that writes the unmasked representation
     */
    void build(StringBuilder builder, Consumer<StringBuilder> build);

    /**
     * Appends an {@link Iterable} to a {@link StringBuilder} by masking each element.
     * @param builder  {@link StringBuilder} to write to
     * @param iterable {@link Iterable} iterable to mask
     */
    default void append(StringBuilder builder, Iterable<? extends Mask.Maskable> iterable) {
        final var appendStringProcedure = new AppendStringProcedure<String>(builder, ", ");
        final var mask = this;
        final var appendMaskedStringProcedure = new Procedure<Mask.Maskable>() {
            @Override
            public void value(Mask.Maskable each) {
                appendStringProcedure.value(each.toString(mask));
            }
        };

        builder.append("[");
        iterable.forEach(appendMaskedStringProcedure);
        builder.append("]");
    }

    Mask NO = new Mask() {
        @Override
        public String lazyFilter(Supplier<String> supplier) {
            return supplier.get();
        }

        @Override
        public void build(StringBuilder builder, Consumer<StringBuilder> build) {
            build.accept(builder);
        }
    };

    Mask YES = new Mask() {
        private static final String PLACEHOLDER = "<MASKED>";

        @Override
        public String lazyFilter(Supplier<String> supplier) {
            return PLACEHOLDER;
        }

        @Override
        public void build(StringBuilder builder, Consumer<StringBuilder> build) {
            builder.append(PLACEHOLDER);
        }
    };

    interface Maskable {
        /**
         * Returns a string representation whose user data fields are optionally masked out.
         *
         * @param mask whether to mask data fields
         * @return string representation
         */
        String toString(Mask mask);
    }
}
