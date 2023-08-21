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
package org.neo4j.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.Arrays;
import java.util.Map;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.string.UTF8;

public class AuthTokenUtil {
    @SuppressWarnings("unchecked")
    public static boolean matches(Map<String, Object> expected, Object actualObject) {
        if (expected == null || actualObject == null) {
            return expected == actualObject;
        }

        if (!(actualObject instanceof Map<?, ?>)) {
            return false;
        }

        Map<String, Object> actual = (Map<String, Object>) actualObject;

        if (expected.size() != actual.size()) {
            return false;
        }

        for (Map.Entry<String, Object> expectedEntry : expected.entrySet()) {
            String key = expectedEntry.getKey();
            Object expectedValue = expectedEntry.getValue();
            Object actualValue = actual.get(key);
            if (AuthToken.containsSensitiveInformation(key)) {
                byte[] expectedByteArray = expectedValue instanceof byte[]
                        ? (byte[]) expectedValue
                        : expectedValue != null ? UTF8.encode((String) expectedValue) : null;
                if (!Arrays.equals(expectedByteArray, (byte[]) actualValue)) {
                    return false;
                }
            } else if (expectedValue == null || actualValue == null) {
                return expectedValue == actualValue;
            } else if (!expectedValue.equals(actualValue)) {
                return false;
            }
        }
        return true;
    }

    public static void assertAuthTokenMatches(Map<String, Object> expected, Map<String, Object> actual) {
        assertFalse(expected == null ^ actual == null);
        assertEquals(expected.keySet(), actual.keySet());
        expected.forEach((key, expectedValue) -> {
            Object actualValue = actual.get(key);
            if (AuthToken.containsSensitiveInformation(key)) {
                byte[] expectedByteArray = expectedValue != null ? UTF8.encode((String) expectedValue) : null;
                assertArrayEquals(expectedByteArray, (byte[]) actualValue);
            } else {
                assertEquals(expectedValue, actualValue);
            }
        });
    }
}
