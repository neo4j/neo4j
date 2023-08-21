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
package org.neo4j.internal.kernel.api.helpers;

import org.neo4j.internal.kernel.api.CloseListener;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.kernel.api.StatementConstants;

public interface CompositeCursor extends Cursor {

    CompositeCursor EMPTY = new CompositeCursor() {
        @Override
        public long reference() {
            return StatementConstants.NO_SUCH_ENTITY;
        }

        @Override
        public boolean next() {
            return false;
        }

        @Override
        public void setTracer(KernelReadTracer tracer) {}

        @Override
        public void removeTracer() {}

        @Override
        public void close() {}

        @Override
        public void closeInternal() {}

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void setCloseListener(CloseListener closeListener) {}

        @Override
        public void setToken(int token) {}

        @Override
        public int getToken() {
            return 0;
        }
    };

    long reference();
}
