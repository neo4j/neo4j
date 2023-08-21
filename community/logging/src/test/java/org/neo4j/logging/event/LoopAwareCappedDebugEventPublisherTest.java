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
package org.neo4j.logging.event;

import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class LoopAwareCappedDebugEventPublisherTest {

    private static final int MAXIMUM_LOG_INTERVAL = 5;
    private static final String MESSAGE_1 = "Some message";
    private static final String MESSAGE_2 = "Some other message";
    private static final String MESSAGE_3 = "A third message!";
    private static final String MESSAGE_4 = "This is too many messages";

    private final DebugEventPublisher mockPublisher = mock(DebugEventPublisher.class);

    private final LoopAwareCappedDebugEventPublisher publisher =
            new LoopAwareCappedDebugEventPublisher(mockPublisher, MAXIMUM_LOG_INTERVAL);

    @ParameterizedTest
    @EnumSource(Type.class)
    void whenSingleEvent_thenShouldLog(Type type) {
        publisher.publish(type, MESSAGE_1);
        var parameters = Parameters.of("key1", "value1");
        publisher.publish(type, MESSAGE_2, parameters);

        var inOrder = inOrder(mockPublisher);
        inOrder.verify(mockPublisher).publish(type, MESSAGE_1);
        inOrder.verify(mockPublisher).publish(type, MESSAGE_2, parameters);
    }

    @Test
    void whenEventRepeatsWithinLoop_thenShouldLog() {
        publisher.publish(Type.Begin, MESSAGE_1);
        publisher.publish(Type.Info, MESSAGE_2);
        publisher.publish(Type.Finish, MESSAGE_3);
        publisher.publish(Type.Info, MESSAGE_2);

        var inOrder = inOrder(mockPublisher);
        inOrder.verify(mockPublisher).publish(Type.Begin, MESSAGE_1);
        inOrder.verify(mockPublisher).publish(Type.Info, MESSAGE_2);
        inOrder.verify(mockPublisher).publish(Type.Finish, MESSAGE_3);
        inOrder.verify(mockPublisher).publish(Type.Info, MESSAGE_2);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void whenWholeLoopIsTheSame_thenShouldNotLog() {
        // Given
        publisher.publish(Type.Begin, MESSAGE_1);
        publisher.publish(Type.Info, MESSAGE_2);
        publisher.publish(Type.Finish, MESSAGE_3);
        publisher.loopComplete();
        reset(mockPublisher);

        // When
        publisher.publish(Type.Begin, MESSAGE_1);
        publisher.publish(Type.Info, MESSAGE_2);
        publisher.publish(Type.Finish, MESSAGE_3);

        publisher.loopComplete();

        // Then
        verifyNoMoreInteractions(mockPublisher);
    }

    @Test
    void whenLoopHasDifferentLogEventAtTheEnd_thenShouldLog() {
        // Given
        publisher.publish(Type.Begin, MESSAGE_1);
        publisher.publish(Type.Info, MESSAGE_2);
        publisher.publish(Type.Finish, MESSAGE_3);
        publisher.loopComplete();
        reset(mockPublisher);

        // When
        publisher.publish(Type.Begin, MESSAGE_1);
        publisher.publish(Type.Info, MESSAGE_2);
        publisher.publish(Type.Warn, MESSAGE_3); // Different type

        publisher.loopComplete();

        // Then
        var inOrder = inOrder(mockPublisher);
        inOrder.verify(mockPublisher).publish(Type.Begin, MESSAGE_1);
        inOrder.verify(mockPublisher).publish(Type.Info, MESSAGE_2);
        inOrder.verify(mockPublisher).publish(Type.Warn, MESSAGE_3);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void whenLoopHasDifferentLogEventAtTheStart_thenShouldLog() {
        // Given
        publisher.publish(Type.Begin, MESSAGE_1);
        publisher.publish(Type.Info, MESSAGE_2);
        publisher.publish(Type.Finish, MESSAGE_3);
        publisher.loopComplete();

        // When
        publisher.publish(Type.Warn, MESSAGE_4);
        publisher.publish(Type.Info, MESSAGE_2);
        publisher.publish(Type.Finish, MESSAGE_3);

        // Then
        var inOrder = inOrder(mockPublisher);
        inOrder.verify(mockPublisher).publish(Type.Begin, MESSAGE_1);
        inOrder.verify(mockPublisher).publish(Type.Info, MESSAGE_2);
        inOrder.verify(mockPublisher).publish(Type.Finish, MESSAGE_3);
        inOrder.verify(mockPublisher).publish(Type.Warn, MESSAGE_4);
        inOrder.verify(mockPublisher).publish(Type.Info, MESSAGE_2);
        inOrder.verify(mockPublisher).publish(Type.Finish, MESSAGE_3);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void whenLoopHasExtraLogEvent_thenShouldLog() {
        // Given
        publisher.publish(Type.Begin, MESSAGE_1);
        publisher.publish(Type.Info, MESSAGE_2);
        publisher.publish(Type.Finish, MESSAGE_3);
        publisher.loopComplete();

        // When
        publisher.publish(Type.Begin, MESSAGE_1);
        publisher.publish(Type.Info, MESSAGE_2);
        publisher.publish(Type.Finish, MESSAGE_3);
        publisher.publish(Type.Warn, MESSAGE_4);

        // Then
        var inOrder = inOrder(mockPublisher);
        inOrder.verify(mockPublisher).publish(Type.Begin, MESSAGE_1);
        inOrder.verify(mockPublisher).publish(Type.Info, MESSAGE_2);
        inOrder.verify(mockPublisher).publish(Type.Finish, MESSAGE_3);
        inOrder.verify(mockPublisher).publish(Type.Begin, MESSAGE_1);
        inOrder.verify(mockPublisher).publish(Type.Info, MESSAGE_2);
        inOrder.verify(mockPublisher).publish(Type.Finish, MESSAGE_3);
        inOrder.verify(mockPublisher).publish(Type.Warn, MESSAGE_4);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void whenLoopHasMissingLogEvent_thenShouldLog() {
        // Given
        publisher.publish(Type.Begin, MESSAGE_1);
        publisher.publish(Type.Info, MESSAGE_2);
        publisher.publish(Type.Finish, MESSAGE_3);
        publisher.loopComplete();

        // When
        publisher.publish(Type.Info, MESSAGE_2);
        publisher.publish(Type.Finish, MESSAGE_3);

        // Then
        var inOrder = inOrder(mockPublisher);
        inOrder.verify(mockPublisher).publish(Type.Begin, MESSAGE_1);
        inOrder.verify(mockPublisher).publish(Type.Info, MESSAGE_2);
        inOrder.verify(mockPublisher).publish(Type.Finish, MESSAGE_3);
        inOrder.verify(mockPublisher).publish(Type.Info, MESSAGE_2);
        inOrder.verify(mockPublisher).publish(Type.Finish, MESSAGE_3);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void whenLoopHasEventWithSameMessageButDifferentParameters_thenShouldLog() {
        // Given
        var parameters1 = Parameters.of("foo", "bar");
        var parameters2 = Parameters.of("foo", "baz");
        publisher.publish(Type.Info, MESSAGE_3, parameters1);
        publisher.loopComplete();
        reset(mockPublisher);

        // When
        publisher.publish(Type.Info, MESSAGE_3, parameters2);

        // Then
        verify(mockPublisher).publish(Type.Info, MESSAGE_3, parameters2);
    }

    @Test
    void whenLoopHasEventWithSameMessageButDifferentLevels_thenShouldLog() {
        publisher.publish(Type.Begin, MESSAGE_1);
        publisher.loopComplete();
        publisher.publish(Type.Info, MESSAGE_1);
        publisher.loopComplete();
        publisher.publish(Type.Warn, MESSAGE_1);

        var inOrder = inOrder(mockPublisher);
        inOrder.verify(mockPublisher).publish(Type.Begin, MESSAGE_1);
        inOrder.verify(mockPublisher).publish(Type.Info, MESSAGE_1);
        inOrder.verify(mockPublisher).publish(Type.Warn, MESSAGE_1);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void whenMoreRepeatsThanTheMaximumInterval_thenShouldLog() {
        // Given
        publisher.publish(Type.Begin, MESSAGE_1);
        publisher.publish(Type.Info, MESSAGE_2);
        publisher.publish(Type.Finish, MESSAGE_3);
        publisher.loopComplete();
        reset(mockPublisher);

        // When
        IntStream.range(0, MAXIMUM_LOG_INTERVAL + 1).forEach(counter -> {
            publisher.publish(Type.Begin, MESSAGE_1);
            publisher.publish(Type.Info, MESSAGE_2);
            publisher.publish(Type.Finish, MESSAGE_3);
            publisher.loopComplete();
        });

        // Then
        verify(mockPublisher).publish(eq(Type.Info), contains(MAXIMUM_LOG_INTERVAL + " repetitions"));
        verify(mockPublisher).publish(Type.Begin, MESSAGE_1);
        verify(mockPublisher).publish(Type.Info, MESSAGE_2);
        verify(mockPublisher).publish(Type.Finish, MESSAGE_3);
        verifyNoMoreInteractions(mockPublisher);
    }
}
