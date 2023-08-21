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
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import org.mockito.Mockito;
import org.neo4j.bolt.tx.error.statement.StatementException;
import org.neo4j.bolt.tx.statement.Statement;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;

public final class StatementMockFactory extends AbstractMockFactory<Statement, StatementMockFactory> {
    private final long id;

    private StatementMockFactory(long id) {
        super(Statement.class);

        this.id = id;
        this.withStaticValue(Statement::id, id);

        // configure an empty result by default so that tests do not need to explicitly configure
        // results in cases where results are irrelevant to verifying functionality
        this.withResults(MockResult.newFactory().build());
    }

    public static StatementMockFactory newFactory(long id) {
        return new StatementMockFactory(id);
    }

    public static StatementMockFactory newFactory() {
        return newFactory(123);
    }

    public static Statement newInstance(long id) {
        return newFactory(id).build();
    }

    public static Statement newInstance() {
        return newInstance(123);
    }

    public static Statement newInstance(long id, Consumer<StatementMockFactory> configurer) {
        var factory = newFactory(id);
        configurer.accept(factory);
        return factory.build();
    }

    public static Statement newInstance(Consumer<StatementMockFactory> configurer) {
        return newInstance(123, configurer);
    }

    public StatementMockFactory withId(long id) {
        return this.withStaticValue(Statement::id, id);
    }

    public StatementMockFactory withFieldNames(List<String> fieldNames) {
        return this.withStaticValue(Statement::fieldNames, fieldNames);
    }

    public StatementMockFactory withFieldNames(String... fieldNames) {
        return this.withFieldNames(Arrays.asList(fieldNames));
    }

    public StatementMockFactory withExecutionTime(long executionTime) {
        return this.withStaticValue(Statement::executionTime, executionTime);
    }

    public StatementMockFactory withQueryStatistics(QueryStatistics statistics) {
        return this.withStaticValue(Statement::statistics, Optional.of(statistics));
    }

    public StatementMockFactory withQueryStatistics(Consumer<QueryStatisticsMockFactory> configurer) {
        return this.withQueryStatistics(QueryStatisticsMockFactory.newInstance(configurer));
    }

    public StatementMockFactory withRemaining(boolean remaining) {
        return this.withStaticValue(Statement::hasRemaining, remaining);
    }

    public StatementMockFactory withListenerList(List<Statement.Listener> listeners) {
        this.with(statement -> {
            Mockito.doAnswer(invocation -> listeners.add(invocation.getArgument(0)))
                    .when(statement)
                    .registerListener(Mockito.any());
            Mockito.doAnswer(invocation -> listeners.remove(invocation.getArgument(0)))
                    .when(statement)
                    .removeListener(Mockito.any());
        });

        return this;
    }

    public StatementMockFactory withResults(MockResult result) {
        this.withAnswer(Statement::fieldNames, invocation -> result.fieldNames());
        this.withAnswer(Statement::hasRemaining, invocation -> result.hasRemaining());

        this.with(statement -> {
            try {
                Mockito.doAnswer(invocation -> {
                            result.consume(invocation.getArgument(0), invocation.<Long>getArgument(1));
                            return null;
                        })
                        .when(statement)
                        .consume(Mockito.any(), Mockito.anyLong());
            } catch (StatementException ignore) {
            }
        });

        this.with(statement -> {
            try {
                Mockito.doAnswer(invocation -> {
                            result.discard(invocation.getArgument(0), invocation.<Long>getArgument(1));
                            return null;
                        })
                        .when(statement)
                        .discard(Mockito.any(), Mockito.anyLong());
            } catch (StatementException ignore) {
            }
        });

        return this;
    }

    public StatementMockFactory withResults(MapValue value) {
        var factory = MockResult.newFactory();

        var record = new ArrayList<AnyValue>();
        value.keySet().forEach(field -> {
            factory.withField(field);
            record.add(value.get(field));
        });

        factory.withRecord(record);

        return this.withResults(factory.build());
    }
}
