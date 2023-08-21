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

import java.util.function.Consumer;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.neo4j.bolt.protocol.common.message.response.SuccessMessage;
import org.neo4j.values.virtual.MapValue;

public class SuccessMessageAssertions extends ResponseMessageAssertions<SuccessMessageAssertions, SuccessMessage> {

    private SuccessMessageAssertions(SuccessMessage actual) {
        super(actual, SuccessMessageAssertions.class);
    }

    public static SuccessMessageAssertions assertThat(SuccessMessage actual) {
        return new SuccessMessageAssertions(actual);
    }

    public static InstanceOfAssertFactory<SuccessMessage, SuccessMessageAssertions> successMessage() {
        return new InstanceOfAssertFactory<>(SuccessMessage.class, SuccessMessageAssertions::assertThat);
    }

    public SuccessMessageAssertions hasMeta(Consumer<MapValue> assertions) {
        this.isNotNull();

        var meta = this.actual.meta();
        if (meta == null) {
            this.failWithMessage("Expected meta field to be present");
        }

        assertions.accept(meta);
        return this;
    }
}
