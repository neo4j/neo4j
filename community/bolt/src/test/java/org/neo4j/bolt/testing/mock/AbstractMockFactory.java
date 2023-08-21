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
package org.neo4j.bolt.testing.mock;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.OngoingStubbing;

/**
 * Provides a basis for factory objects which construct mock instances according to a variable set of properties.
 *
 * @param <T> a mock type.
 * @param <SELF> an implementation of this factory type.
 */
public abstract class AbstractMockFactory<T, SELF extends AbstractMockFactory<T, SELF>> {
    private final Class<? extends T> type;
    private final Answer<T> defaultAnswer;
    private final List<Consumer<T>> configurators = new ArrayList<>();

    protected AbstractMockFactory(Class<? extends T> type, Answer<T> defaultAnswer) {
        this.type = type;
        this.defaultAnswer = defaultAnswer;
    }

    @SuppressWarnings("unchecked")
    protected AbstractMockFactory(Class<? extends T> type) {
        this(type, (Answer<T>) Mockito.RETURNS_MOCKS);
    }

    /**
     * Constructs a new instance of the desired target type using the set of configuration parameters previously passed
     * to this factory.
     *
     * @return a mock object.
     */
    public T build() {
        var mock = Mockito.mock(this.type, this.defaultAnswer);

        this.configurators.forEach(configurator -> configurator.accept(mock));

        return mock;
    }

    /**
     * Applies the configuration within this factory to a given mock.
     *
     * @param mock a mock object.
     * @return a reference to the given mock object.
     */
    public T apply(T mock) {
        this.configurators.forEach(configurator -> configurator.accept(mock));
        return mock;
    }

    @SuppressWarnings("unchecked")
    public SELF with(Consumer<T> configurator) {
        this.configurators.add(configurator);
        return (SELF) this;
    }

    public <R> SELF when(Function<T, R> call, Consumer<OngoingStubbing<R>> stub) {
        return this.with(mock -> {
            var stubbing = Mockito.when(call.apply(mock));
            stub.accept(stubbing);
        });
    }

    public SELF nothingWhen(Consumer<T> call) {
        return this.with(mock -> {
            var tmp = Mockito.doNothing().when(mock);
            call.accept(tmp);
        });
    }

    @SuppressWarnings("rawtypes")
    public <E extends Throwable> SELF withAnswer(Consumer<T> call, Answer answer) {
        return this.with(mock -> {
            var tmp = Mockito.doAnswer(answer).when(mock);

            call.accept(tmp);
        });
    }

    public <R> SELF withStaticValue(Function<T, R> call, R value) {
        return this.when(call, stubbing -> stubbing.thenReturn(value));
    }

    public <A, R> ArgumentCaptor<A> withArgumentCaptor(
            Class<A> type, BiFunction<T, A, R> call, Consumer<OngoingStubbing<R>> stub) {
        var captor = ArgumentCaptor.forClass(type);
        this.when(mock -> call.apply(mock, captor.capture()), stub);
        return captor;
    }

    public <A, R> ArgumentCaptor<A> withArgumentCaptor(Class<A> type, BiConsumer<T, A> call) {
        var captor = ArgumentCaptor.forClass(type);
        this.nothingWhen(mock -> call.accept(mock, captor.capture()));
        return captor;
    }
}
