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
package org.neo4j.internal.helpers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ExceptionsTest {
    @Test
    void shouldDetectContainsOneOfSome() {
        // GIVEN
        var cause = new ARuntimeException(new AnotherRuntimeException(new NullPointerException("Some words")));

        // THEN
        assertTrue(Exceptions.contains(cause, "words", NullPointerException.class));
        assertFalse(Exceptions.contains(cause, "not", NullPointerException.class));
    }

    @Test
    void shouldChainExceptionsWhenInitialExceptionIsNull() {
        var exception = new RuntimeException();

        var chainedException = Exceptions.chain(null, exception);

        assertSame(exception, chainedException);
    }

    @Test
    void shouldChainExceptionsWhenCurrentExceptionIsNull() {
        var exception = new RuntimeException();

        var chainedException = Exceptions.chain(exception, null);

        assertSame(exception, chainedException);
        assertThat(chainedException.getSuppressed()).isEmpty();
    }

    @Test
    void shouldChainExceptions() {
        var exception1 = new RuntimeException();
        var exception2 = new RuntimeException();

        var chainedException = Exceptions.chain(exception1, exception2);

        assertSame(exception1, chainedException);
        assertThat(chainedException.getSuppressed()).containsExactly(exception2);
    }

    @Test
    void shouldNotChainSameException() {
        var exception = new RuntimeException();

        var chainedException = Exceptions.chain(exception, exception);

        assertSame(exception, chainedException);
        assertThat(chainedException.getSuppressed()).isEmpty();
    }

    @Test
    void shouldThrowAsIsIfCorrectCheckedInInstanceOfOrUnchecked() {
        // GIVEN
        var exception = new LevelOneException("The one exception");

        // THEN
        assertThatThrownBy(() -> Exceptions.throwIfInstanceOfOrUnchecked(exception, LevelOneException.class))
                .isSameAs(exception);
    }

    @Test
    void shouldThrowAsIsIfUncheckedInInstanceOfOrUnchecked() {
        // GIVEN
        var exception = new ARuntimeException(new LevelOneException("The one exception"));

        // THEN
        assertThatThrownBy(() -> Exceptions.throwIfInstanceOfOrUnchecked(exception, LevelOneException.class))
                .isSameAs(exception);
    }

    @Test
    void shouldThrowAsFallbackIfNeitherInInstanceOfOrUnchecked() {
        // GIVEN
        var exception = new LevelOneException("The one exception");

        // THEN
        assertThatThrownBy(() -> Exceptions.throwIfInstanceOfOrUnchecked(
                        exception, AnotherCheckedException.class, AnotherCheckedException::new))
                .hasCause(exception);
    }

    private static class LevelOneException extends Exception {
        LevelOneException(String message) {
            super(message);
        }
    }

    private static class AnotherCheckedException extends Exception {
        AnotherCheckedException(Throwable cause) {
            super(cause);
        }
    }

    private static class ARuntimeException extends RuntimeException {
        ARuntimeException(Throwable cause) {
            super(cause);
        }
    }

    private static class AnotherRuntimeException extends RuntimeException {
        AnotherRuntimeException(Throwable cause) {
            super(cause);
        }
    }
}
