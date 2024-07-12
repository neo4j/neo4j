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
package org.neo4j.genai.util;

import static org.assertj.core.api.Fail.fail;

import org.assertj.core.api.AbstractThrowableAssert;
import org.neo4j.function.ThrowingSupplier;

public class ThrowingSupplierAssert<SELF, ACTUAL extends Throwable>
        extends AbstractThrowableAssert<ThrowingSupplierAssert<SELF, ACTUAL>, ACTUAL> {
    protected ThrowingSupplierAssert(ACTUAL actual) {
        super(actual, ThrowingSupplierAssert.class);
        hasBeenThrown();
    }

    public ThrowingSupplierAssert<?, ? extends Throwable> findFirstInstanceOfInCauseChain(Class<?> cls) {
        Throwable throwable = this.actual;
        while (!cls.isInstance(throwable)) {
            if (throwable == null) {
                fail(
                        "Expected %s to have a cause that is an instance of '%s' somewhere in its cause chain.",
                        Throwable.class.getSimpleName(), cls.getName());
            }

            final var cause = throwable.getCause();
            if (cause == throwable) {
                break;
            }
            throwable = cause;
        }
        return new ThrowingSupplierAssert<>(throwable);
    }

    public static ThrowingSupplierAssert<?, ? extends Throwable> assertThatThrownBy(
            ThrowingSupplier<?, ?> shouldRaiseThrowable) {
        Throwable throwable = null;
        try {
            shouldRaiseThrowable.get();
        } catch (Throwable t) {
            throwable = t;
        }
        return new ThrowingSupplierAssert<>(throwable);
    }
}
