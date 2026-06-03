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
package org.neo4j.fleetmanagement.queries.model;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class SimplifiedGqlErrorTest {

    @Test
    void shouldBeEqual() {
        SimplifiedGqlError error1 = new SimplifiedGqlError();
        error1.classification = "CLIENT_ERROR";
        error1.statusDescription = "message";
        error1.gqlStatus = "01000";

        SimplifiedGqlError error2 = new SimplifiedGqlError();
        error2.classification = "CLIENT_ERROR";
        error2.statusDescription = "message";
        error2.gqlStatus = "01000";

        assertThat(error2).isEqualTo(error1);
        assertThat(error2).hasSameHashCodeAs(error1);
    }

    @Test
    void shouldNotBeEqual() {
        SimplifiedGqlError error1 = new SimplifiedGqlError();
        error1.classification = "CLIENT_ERROR";
        error1.statusDescription = "message1";
        error1.gqlStatus = "01000";

        SimplifiedGqlError error2 = new SimplifiedGqlError();
        error2.classification = "CLIENT_ERROR";
        error2.statusDescription = "message2";
        error2.gqlStatus = "01000";

        assertThat(error2).isNotEqualTo(error1);
    }

    @Test
    void shouldBeEqualWithCause() {
        SimplifiedGqlError cause1 = new SimplifiedGqlError();
        cause1.statusDescription = "cause";
        SimplifiedGqlError error1 = new SimplifiedGqlError();
        error1.cause = cause1;

        SimplifiedGqlError cause2 = new SimplifiedGqlError();
        cause2.statusDescription = "cause";
        SimplifiedGqlError error2 = new SimplifiedGqlError();
        error2.cause = cause2;

        assertThat(error2).isEqualTo(error1);
        assertThat(error2).hasSameHashCodeAs(error1);
    }

    @Test
    void shouldHaveToString() {
        SimplifiedGqlError error = new SimplifiedGqlError();
        error.classification = "CLIENT_ERROR";
        error.statusDescription = "message";
        error.gqlStatus = "01000";

        assertThat(error)
                .hasToString(
                        "SimplifiedGqlError{cause=null, classification='CLIENT_ERROR', message='message', gqlStatus='01000'}");
    }
}
