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
package org.neo4j.internal.kernel.api.exceptions.schema;

import org.junit.jupiter.api.Test;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectAssertions;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.values.storable.StringValue;
import org.neo4j.values.storable.Values;

class MalformedSchemaRuleExceptionTest {

    @Test
    void propertyMismatchShouldUseCorrectMessageAndGqlStatus() {
        MalformedSchemaRuleException e =
                MalformedSchemaRuleException.propertyTypeMismatch("prop", Values.intValue(1), StringValue.class);
        ErrorGqlStatusObjectAssertions.assertThat(e)
                .hasMessageContaining("Expected property prop to be a StringValue but was Int(1)")
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22N01)
                .hasStatusDescription(
                        "error: data exception - invalid type. Expected the value 1 to be of type StringValue, but was of type IntValue.");
    }
}
