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
package org.neo4j.procedure.impl.temporal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.neo4j.kernel.api.procedure.BasicContext.buildContext;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.stringValue;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.collection.Dependencies;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.QueryLanguage;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.procedure.impl.GlobalProceduresRegistry;
import org.neo4j.procedure.impl.ProcedureConfig;
import org.neo4j.values.AnyValue;

class TemporalFunctionTest {
    @ParameterizedTest
    @MethodSource("truncateFunctions")
    void truncateShouldReturnNoValueForNoValueInput(QualifiedName functionName, String unit) throws ProcedureException {
        assertThat(callFunction(functionName, stringValue(unit), NO_VALUE, NO_VALUE))
                .isEqualTo(NO_VALUE);
    }

    @ParameterizedTest
    @MethodSource("truncateFunctions")
    void truncateShouldReturnNoValueForNullInput(QualifiedName functionName, String unit) throws ProcedureException {
        assertThat(callFunction(functionName, stringValue(unit), null, NO_VALUE))
                .isEqualTo(NO_VALUE);
    }

    private static Stream<Arguments> truncateFunctions() {
        return Stream.of(
                Arguments.of(new QualifiedName("date", "truncate"), "day"),
                Arguments.of(new QualifiedName("datetime", "truncate"), "minute"),
                Arguments.of(new QualifiedName("localdatetime", "truncate"), "minute"),
                Arguments.of(new QualifiedName("localtime", "truncate"), "minute"),
                Arguments.of(new QualifiedName("time", "truncate"), "minute"));
    }

    private static AnyValue callFunction(QualifiedName name, AnyValue... arguments) throws ProcedureException {
        var procedures = new GlobalProceduresRegistry();
        TemporalFunction.registerTemporalFunctions(procedures, ProcedureConfig.DEFAULT);

        var view = procedures.getCurrentView();
        var functionId = view.function(name, QueryLanguage.CYPHER_5).id();
        return view.callFunction(context(), functionId, arguments);
    }

    private static Context context() {
        return buildContext(new Dependencies(), new DefaultValueMapper(mock(InternalTransaction.class)))
                .context();
    }
}
