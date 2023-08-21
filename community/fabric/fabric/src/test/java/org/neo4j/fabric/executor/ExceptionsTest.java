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
package org.neo4j.fabric.executor;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.api.exceptions.Status;

class ExceptionsTest {
    @Test
    void testCompositeExceptionWithSomePrimaryErrors() {
        var primary1 = new FabricException(Status.General.UnknownError, "msg-1");
        var primary2 = new FabricException(Status.General.UnknownError, "msg-2");
        var secondary = new FabricSecondaryException(
                Status.General.UnknownError,
                "msg-3",
                new IllegalStateException("msg-4"),
                new FabricException(Status.General.UnknownError, "msg-5"));

        var reactorException = reactor.core.Exceptions.multiple(primary1, primary2, secondary);
        var transformedException = Exceptions.transform(Status.General.UnknownError, reactorException);
        assertThat(unpackExceptionMessages(transformedException)).contains("msg-1", "msg-2");
    }

    @Test
    void testCompositeExceptionWithOnlySecondaryErrors() {
        var sharedPrimary = new FabricException(Status.General.UnknownError, "msg-1");

        var secondary1 = new FabricSecondaryException(
                Status.General.UnknownError, "msg-2", new IllegalStateException("msg-3"), sharedPrimary);
        var secondary2 = new FabricSecondaryException(
                Status.General.UnknownError,
                "msg-4",
                new IllegalStateException("msg-5"),
                new FabricException(Status.General.UnknownError, "msg-6"));
        var secondary3 = new FabricSecondaryException(
                Status.General.UnknownError, "msg-7", new IllegalStateException("msg-8"), sharedPrimary);

        var reactorException = reactor.core.Exceptions.multiple(secondary1, secondary2, secondary3);
        var transformedException = Exceptions.transform(Status.General.UnknownError, reactorException);
        assertThat(unpackExceptionMessages(transformedException)).contains("msg-1", "msg-6");
    }

    private static List<String> unpackExceptionMessages(Exception exception) {
        List<String> messages = new ArrayList<>();
        messages.add(exception.getMessage());
        Arrays.stream(exception.getSuppressed()).map(Throwable::getMessage).forEach(messages::add);
        return messages;
    }
}
