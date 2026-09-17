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
package org.neo4j.kernel.impl.api;

import static org.apache.commons.lang3.RandomStringUtils.insecure;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.gqlstatus.ErrorGqlStatusObjectAssertions.assertThatThrownBy;
import static org.neo4j.gqlstatus.GqlStatusInfoCodes.STATUS_22NBH;
import static org.neo4j.kernel.api.impl.schema.LuceneTestTokenNameLookup.SIMPLE_TOKEN_LOOKUP;
import static org.neo4j.kernel.impl.api.LuceneIndexValueValidator.MAX_TERM_LENGTH;
import static org.neo4j.values.storable.Values.of;

import org.junit.jupiter.api.Test;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.api.index.IndexValueValidator;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomSupportExtension;
import org.neo4j.values.ElementIdMapper;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

@RandomSupportExtension
class LuceneIndexValueValidatorTest {
    private static final IndexDescriptor descriptor = IndexPrototype.forSchema(SchemaDescriptors.forLabel(1, 1))
            .withName("test")
            .materialise(1);
    private static final IndexValueValidator VALIDATOR =
            new LuceneIndexValueValidator(descriptor, SIMPLE_TOKEN_LOOKUP, ElementIdMapper.PLACEHOLDER);
    private static final long ENTITY_ID = 42;

    @Inject
    RandomSupport random;

    @Test
    void tooLongArrayIsNotAllowed() {
        assertThatThrownBy(() -> {
                    TextArray largeArray = Values.stringArray(
                            insecure().nextAlphabetic(MAX_TERM_LENGTH),
                            insecure().nextAlphabetic(MAX_TERM_LENGTH));
                    VALIDATOR.validate(ENTITY_ID, largeArray);
                })
                .hasGqlStatus(STATUS_22NBH)
                .hasMessageContaining("is too large for the index");
    }

    @Test
    void stringOverExceedLimitNotAllowed() {
        int length = MAX_TERM_LENGTH + 1;
        assertThatThrownBy(() -> VALIDATOR.validate(ENTITY_ID, values(insecure().nextAlphabetic(length))))
                .hasGqlStatus(STATUS_22NBH)
                .hasMessageContainingAll(
                        "The property value size",
                        String.valueOf(length * Byte.BYTES),
                        "of element id",
                        "is too large for the index",
                        "See the Operations Manual for index limitations.");
    }

    @Test
    void nullIsNotAllowed() {
        IllegalArgumentException iae = assertThrows(
                IllegalArgumentException.class, () -> VALIDATOR.validate(ENTITY_ID, values((Object) null)));
        assertEquals("Null value", iae.getMessage());
    }

    @Test
    void numberIsValidValue() {
        VALIDATOR.validate(ENTITY_ID, values(5));
        VALIDATOR.validate(ENTITY_ID, values(5.0d));
        VALIDATOR.validate(ENTITY_ID, values(5.0f));
        VALIDATOR.validate(ENTITY_ID, values(5L));
    }

    @Test
    void shortArrayIsValidValue() {
        VALIDATOR.validate(ENTITY_ID, values((Object) new long[] {1, 2, 3}));
        VALIDATOR.validate(ENTITY_ID, values((Object) random.nextBytes(200)));
    }

    @Test
    void shortStringIsValidValue() {
        VALIDATOR.validate(ENTITY_ID, values(insecure().nextAlphabetic(5)));
        VALIDATOR.validate(ENTITY_ID, values(insecure().nextAlphabetic(10)));
        VALIDATOR.validate(ENTITY_ID, values(insecure().nextAlphabetic(250)));
        VALIDATOR.validate(ENTITY_ID, values(insecure().nextAlphabetic(450)));
        VALIDATOR.validate(ENTITY_ID, values(insecure().nextAlphabetic(MAX_TERM_LENGTH)));
    }

    private static Value[] values(Object... objects) {
        Value[] array = new Value[objects.length];
        for (int i = 0; i < objects.length; i++) {
            array[i] = of(objects[i]);
        }
        return array;
    }
}
