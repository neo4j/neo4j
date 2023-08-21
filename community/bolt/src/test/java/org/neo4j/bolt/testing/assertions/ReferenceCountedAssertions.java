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
package org.neo4j.bolt.testing.assertions;

import io.netty.util.ReferenceCounted;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.InstanceOfAssertFactory;

/**
 * Provides conditions which evaluate the state of netty {@link ReferenceCounted} objects within Assert4j.
 */
public abstract class ReferenceCountedAssertions<
                SELF extends ReferenceCountedAssertions<SELF, ACTUAL>, ACTUAL extends ReferenceCounted>
        extends AbstractAssert<SELF, ACTUAL> {

    protected ReferenceCountedAssertions(ACTUAL referenceCounted, Class<SELF> selfType) {
        super(referenceCounted, selfType);
    }

    public static GenericAssertions assertThat(ReferenceCounted value) {
        return new GenericAssertions(value);
    }

    public static InstanceOfAssertFactory<ReferenceCounted, GenericAssertions> referenceCounted() {
        return new InstanceOfAssertFactory<>(ReferenceCounted.class, GenericAssertions::new);
    }

    @SuppressWarnings("unchecked")
    public SELF hasReferences(int expected) {
        this.isNotNull();

        if (this.actual.refCnt() != expected) {
            failWithActualExpectedAndMessage(
                    this.actual.refCnt(),
                    expected,
                    "Expected <%d> references to be held but got <%d>",
                    expected,
                    this.actual.refCnt());
        }

        return (SELF) this;
    }

    @SuppressWarnings("unchecked")
    public SELF hasBeenReleased() {
        this.isNotNull();

        if (this.actual.refCnt() != 0) {
            failWithMessage("Expected object to be released but got <%d> remaining references", this.actual.refCnt());
        }

        return (SELF) this;
    }

    public static class GenericAssertions extends ReferenceCountedAssertions<GenericAssertions, ReferenceCounted> {

        private GenericAssertions(ReferenceCounted referenceCounted) {
            super(referenceCounted, GenericAssertions.class);
        }
    }
}
