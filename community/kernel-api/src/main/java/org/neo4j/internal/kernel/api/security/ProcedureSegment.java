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
package org.neo4j.internal.kernel.api.security;

import java.util.Objects;

public class ProcedureSegment implements Segment {
    private final String procedure;

    public ProcedureSegment(String procedure) {
        this.procedure = procedure;
    }

    public String getProcedure() {
        return procedure;
    }

    @Override
    public boolean satisfies(Segment segment) {
        if (segment instanceof ProcedureSegment other) {
            return procedure == null || procedure.equals(other.procedure);
        }
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ProcedureSegment that = (ProcedureSegment) o;

        return Objects.equals(procedure, that.procedure);
    }

    @Override
    public int hashCode() {
        return procedure != null ? procedure.hashCode() : 0;
    }

    @Override
    public String toString() {
        return procedure == null ? "*" : procedure;
    }

    public static final ProcedureSegment ALL = new ProcedureSegment(null);
}
