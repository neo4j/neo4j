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

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.BooleanAssert;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.assertj.core.api.StringAssert;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.TextValue;

public final class AnyValueAssertions extends AbstractAssert<AnyValueAssertions, AnyValue> {

    AnyValueAssertions(AnyValue anyValue) {
        super(anyValue, AnyValueAssertions.class);
    }

    public static AnyValueAssertions assertThat(AnyValue value) {
        return new AnyValueAssertions(value);
    }

    public static InstanceOfAssertFactory<BooleanValue, BooleanAssert> booleanValue() {
        return new InstanceOfAssertFactory<>(BooleanValue.class, value -> new BooleanAssert(value.booleanValue()));
    }

    public BooleanAssert asBoolean() {
        if (!(this.actual instanceof BooleanValue)) {
            failWithMessage("Expected boolean value but got <%s>", this.actual);
        }

        return new BooleanAssert(((BooleanValue) this.actual).booleanValue());
    }

    @Override
    public StringAssert asString() {
        if (!(this.actual instanceof TextValue)) {
            failWithMessage("Expected string value but got <%s>", this.actual);
        }

        return new StringAssert(((TextValue) this.actual).stringValue());
    }

    public AnyValueAssertions isEqualTo(boolean expected) {
        this.asBoolean().isEqualTo(expected);

        return this;
    }

    public AnyValueAssertions isEqualTo(String expected) {
        this.asString().isEqualTo(expected);

        return this;
    }
}
