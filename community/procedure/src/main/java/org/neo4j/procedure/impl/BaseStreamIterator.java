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
package org.neo4j.procedure.impl;

import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;

import java.util.Iterator;
import java.util.stream.Stream;
import org.neo4j.collection.ResourceRawIterator;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.ResourceMonitor;
import org.neo4j.kernel.api.exceptions.ResourceCloseFailureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.values.AnyValue;

/**
 * Base iterator class used extended by generated code to map from procedure streams
 */
@SuppressWarnings("WeakerAccess")
public abstract class BaseStreamIterator implements ResourceRawIterator<AnyValue[], ProcedureException> {
    private final Iterator<?> out;
    private Stream<?> stream;
    private final ResourceMonitor resourceMonitor;
    private final ProcedureSignature signature;

    public BaseStreamIterator(Stream<?> stream, ResourceMonitor resourceMonitor, ProcedureSignature signature) {
        this.out = stream.iterator();
        this.stream = stream;
        this.resourceMonitor = resourceMonitor;
        this.signature = signature;
        resourceMonitor.registerCloseableResource(stream);
    }

    public abstract AnyValue[] map(Object in);

    @Override
    public boolean hasNext() throws ProcedureException {
        try {
            boolean hasNext = out.hasNext();
            if (!hasNext) {
                close();
            }
            return hasNext;
        } catch (Throwable throwable) {
            throw closeAndCreateProcedureException(throwable);
        }
    }

    @Override
    public AnyValue[] next() throws ProcedureException {
        try {
            Object record = out.next();
            return map(record);
        } catch (Throwable throwable) {
            throw closeAndCreateProcedureException(throwable);
        }
    }

    @Override
    public void close() {
        if (stream != null) {
            // Make sure we reset closeableResource before doing anything which may throw an exception that may
            // result in a recursive call to this close-method
            AutoCloseable resourceToClose = stream;
            stream = null;
            IOUtils.close(
                    ResourceCloseFailureException::new,
                    () -> resourceMonitor.unregisterCloseableResource(resourceToClose),
                    resourceToClose);
        }
    }

    private ProcedureException closeAndCreateProcedureException(Throwable t) {
        ProcedureException procedureException = newProcedureException(t);

        try {
            close();
        } catch (Exception exceptionDuringClose) {
            try {
                procedureException.addSuppressed(exceptionDuringClose);
            } catch (Throwable ignore) {
            }
        }
        return procedureException;
    }

    private ProcedureException newProcedureException(Throwable throwable) {
        if (throwable instanceof Status.HasStatus) {
            return new ProcedureException(((Status.HasStatus) throwable).status(), throwable, throwable.getMessage());
        } else {
            Throwable cause = getRootCause(throwable);
            return new ProcedureException(
                    Status.Procedure.ProcedureCallFailed,
                    throwable,
                    "Failed to invoke procedure `%s`: %s",
                    signature.name(),
                    "Caused by: " + (cause != null ? cause : throwable));
        }
    }
}
