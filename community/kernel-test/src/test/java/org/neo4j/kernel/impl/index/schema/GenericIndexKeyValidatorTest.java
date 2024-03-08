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
package org.neo4j.kernel.impl.index.schema;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.schema.SchemaTestUtil.SIMPLE_NAME_LOOKUP;
import static org.neo4j.values.storable.DateValue.epochDate;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.stringValue;

import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.values.storable.Value;

@ExtendWith(RandomExtension.class)
class GenericIndexKeyValidatorTest {
    private final IndexDescriptor descriptor = IndexPrototype.forSchema(SchemaDescriptors.forLabel(1, 1))
            .withName("test")
            .materialise(1);

    @Inject
    private RandomSupport random;

    @Test
    void shouldNotBotherSerializingToRealBytesIfFarFromThreshold() {
        // given
        RangeLayout layout = mock(RangeLayout.class);
        doThrow(RuntimeException.class).when(layout).newKey();
        GenericIndexKeyValidator validator = new GenericIndexKeyValidator(120, descriptor, layout, SIMPLE_NAME_LOOKUP);

        // when
        validator.validate(42, intValue(10), epochDate(100), stringValue("abc"));

        // then no exception should have been thrown
    }

    @Test
    void shouldInvolveSerializingToRealBytesIfMayCrossThreshold() {
        // given
        RangeLayout layout = mock(RangeLayout.class);
        when(layout.newKey()).thenReturn(new CompositeRangeKey(3));
        GenericIndexKeyValidator validator = new GenericIndexKeyValidator(48, descriptor, layout, SIMPLE_NAME_LOOKUP);

        // when
        var e = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(42, intValue(10), epochDate(100), stringValue("abcdefghijklmnopqrstuvw")));
        assertThat(e.getMessage()).contains("Property value is too large to index");
        verify(layout).newKey();
    }

    @Test
    void shouldReportCorrectValidationErrorsOnRandomlyGeneratedValues() {
        // given
        int slots = random.nextInt(1, 6);
        int maxLength = random.nextInt(15, 30) * slots;
        RangeLayout layout = new RangeLayout(slots);
        GenericIndexKeyValidator validator =
                new GenericIndexKeyValidator(maxLength, descriptor, layout, SIMPLE_NAME_LOOKUP);
        RangeKey key = layout.newKey();

        for (int i = 0; i < 100; i++) {
            // when
            Value[] tuple = generateValueTuple(slots);
            boolean isOk;
            try {
                validator.validate(42, tuple);
                isOk = true;
            } catch (IllegalArgumentException e) {
                isOk = false;
            }
            int actualSize = actualSize(tuple, key);
            boolean manualIsOk = actualSize <= maxLength;

            // then
            if (manualIsOk != isOk) {
                fail(format(
                        "Validator not validating %s correctly. Manual validation on actual key resulted in %b whereas validator said %b",
                        Arrays.toString(tuple), manualIsOk, isOk));
            }
        }
    }

    private static int actualSize(Value[] tuple, RangeKey key) {
        key.initialize(0);
        for (int i = 0; i < tuple.length; i++) {
            key.initFromValue(i, tuple[i], NativeIndexKey.Inclusion.NEUTRAL);
        }
        return key.size();
    }

    private Value[] generateValueTuple(int slots) {
        Value[] tuple = new Value[slots];
        for (int j = 0; j < slots; j++) {
            tuple[j] = random.nextValue();
        }
        return tuple;
    }
}
