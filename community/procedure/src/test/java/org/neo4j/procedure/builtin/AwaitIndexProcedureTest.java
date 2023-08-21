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
package org.neo4j.procedure.builtin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.InternalIndexState.FAILED;
import static org.neo4j.internal.kernel.api.InternalIndexState.ONLINE;
import static org.neo4j.internal.kernel.api.InternalIndexState.POPULATING;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;

class AwaitIndexProcedureTest {
    private static final int TIMEOUT = 10;
    private static final TimeUnit TIME_UNIT = TimeUnit.SECONDS;
    private SchemaRead schemaRead;
    private IndexProcedures procedure;
    private IndexDescriptor anyIndex;

    @BeforeEach
    void setup() throws LabelNotFoundKernelException, PropertyKeyIdNotFoundKernelException {
        final int labelId = 0;
        final int propId = 0;
        LabelSchemaDescriptor anyDescriptor = SchemaDescriptors.forLabel(labelId, propId);
        anyIndex = forSchema(anyDescriptor).withName("index").materialise(13);
        KernelTransaction transaction = mock(KernelTransaction.class);
        schemaRead = mock(SchemaRead.class);
        when(transaction.schemaRead()).thenReturn(schemaRead);
        TokenRead tokenRead = mock(TokenRead.class);
        when(tokenRead.nodeLabelName(labelId)).thenReturn("label_0");
        when(tokenRead.propertyKeyName(propId)).thenReturn("prop_0");
        when(tokenRead.labelGetName(labelId)).thenReturn("label_0");
        when(tokenRead.propertyKeyGetName(propId)).thenReturn("prop_0");
        when(transaction.tokenRead()).thenReturn(tokenRead);
        procedure = new IndexProcedures(transaction, null);
    }

    @Test
    void shouldLookUpTheIndexByIndexName() throws ProcedureException, IndexNotFoundKernelException {
        when(schemaRead.indexGetForName("my index")).thenReturn(anyIndex);
        when(schemaRead.indexGetState(any(IndexDescriptor.class))).thenReturn(ONLINE);

        procedure.awaitIndexByName("my index", TIMEOUT, TIME_UNIT);

        verify(schemaRead).indexGetForName("my index");
    }

    @Test
    void shouldThrowAnExceptionIfTheIndexHasFailed() throws IndexNotFoundKernelException {
        when(schemaRead.indexGetForName(anyString())).thenReturn(anyIndex);
        when(schemaRead.indexGetState(any(IndexDescriptor.class))).thenReturn(FAILED);
        when(schemaRead.indexGetFailure(any(IndexDescriptor.class)))
                .thenReturn(Exceptions.stringify(new Exception("Kilroy was here")));

        ProcedureException exception =
                assertThrows(ProcedureException.class, () -> procedure.awaitIndexByName("index", TIMEOUT, TIME_UNIT));
        assertThat(exception.status()).isEqualTo(Status.Schema.IndexCreationFailed);
        assertThat(exception.getMessage()).contains("Kilroy was here");
        assertThat(exception.getMessage()).contains("Index 'index' is in failed state.: Cause of failure:");
    }

    @Test
    void shouldThrowAnExceptionIfTheIndexDoesNotExist() {
        when(schemaRead.indexGetForName(anyString())).thenReturn(IndexDescriptor.NO_INDEX);

        ProcedureException exception =
                assertThrows(ProcedureException.class, () -> procedure.awaitIndexByName("index", TIMEOUT, TIME_UNIT));
        assertThat(exception.status()).isEqualTo(Status.Schema.IndexNotFound);
    }

    @Test
    void shouldThrowAnExceptionIfTheIndexWithGivenNameDoesNotExist() {
        when(schemaRead.indexGetForName("some index")).thenReturn(IndexDescriptor.NO_INDEX);

        ProcedureException exception = assertThrows(
                ProcedureException.class, () -> procedure.awaitIndexByName("some index", TIMEOUT, TIME_UNIT));
        assertThat(exception.status()).isEqualTo(Status.Schema.IndexNotFound);
    }

    @Test
    void shouldBlockUntilTheIndexIsOnline() throws IndexNotFoundKernelException, InterruptedException {
        when(schemaRead.index(any(SchemaDescriptor.class))).thenReturn(Iterators.iterator(anyIndex));
        when(schemaRead.indexGetForName(anyString())).thenReturn(anyIndex);

        AtomicReference<InternalIndexState> state = new AtomicReference<>(POPULATING);
        when(schemaRead.indexGetState(any(IndexDescriptor.class))).then(invocationOnMock -> state.get());

        AtomicBoolean done = new AtomicBoolean(false);
        var thread = new Thread(() -> {
            try {
                procedure.awaitIndexByName("index", TIMEOUT, TIME_UNIT);
            } catch (ProcedureException e) {
                throw new RuntimeException(e);
            }
            done.set(true);
        });
        thread.start();

        assertThat(done.get()).isFalse();

        state.set(ONLINE);
        thread.join();

        assertThat(done.get()).isTrue();
    }

    @Test
    void shouldTimeoutIfTheIndexTakesTooLongToComeOnline() throws IndexNotFoundKernelException, InterruptedException {
        when(schemaRead.indexGetForName(anyString())).thenReturn(anyIndex);
        when(schemaRead.indexGetState(any(IndexDescriptor.class))).thenReturn(POPULATING);

        AtomicReference<ProcedureException> exception = new AtomicReference<>();
        var thread = new Thread(() -> {
            try {
                // We wait here, because we expect timeout
                procedure.awaitIndexByName("index", 0, TIME_UNIT);
            } catch (ProcedureException e) {
                exception.set(e);
            }
        });
        thread.start();

        thread.join();

        assertThat(exception.get()).isNotNull();
        assertThat(exception.get().status()).isEqualTo(Status.Procedure.ProcedureTimedOut);
    }
}
