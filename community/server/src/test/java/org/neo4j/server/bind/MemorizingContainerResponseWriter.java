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
package org.neo4j.server.bind;

import static java.io.OutputStream.nullOutputStream;
import static javax.ws.rs.core.Response.Status;

import java.io.OutputStream;
import java.util.concurrent.TimeUnit;
import org.glassfish.jersey.server.ContainerException;
import org.glassfish.jersey.server.ContainerResponse;
import org.glassfish.jersey.server.spi.ContainerResponseWriter;

public class MemorizingContainerResponseWriter implements ContainerResponseWriter {
    private Status status;
    private Object entity;

    @Override
    public OutputStream writeResponseStatusAndHeaders(long contentLength, ContainerResponse responseContext)
            throws ContainerException {
        status = Status.fromStatusCode(responseContext.getStatus());
        entity = responseContext.getEntity();
        return nullOutputStream();
    }

    @Override
    public boolean suspend(long timeOut, TimeUnit timeUnit, TimeoutHandler timeoutHandler) {
        return false;
    }

    @Override
    public void setSuspendTimeout(long timeOut, TimeUnit timeUnit) throws IllegalStateException {}

    @Override
    public void commit() {}

    @Override
    public void failure(Throwable error) {}

    @Override
    public boolean enableResponseBuffering() {
        return false;
    }

    public Status getStatus() {
        return status;
    }

    public Object getEntity() {
        return entity;
    }
}
