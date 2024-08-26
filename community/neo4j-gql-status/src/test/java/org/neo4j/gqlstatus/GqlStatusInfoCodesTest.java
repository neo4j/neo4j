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
package org.neo4j.gqlstatus;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.commons.codec.digest.DigestUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class GqlStatusInfoCodesTest {

    @Test
    void verifyParametersCorrectlyWritten() {
        final var allUniqueParams = new EnumMap<GqlMessageParams, Object>(GqlMessageParams.class);
        for (final var p : GqlMessageParams.values()) {
            allUniqueParams.put(p, "⚠️very-unique-param-value-%s⚠️".formatted(p.name()));
        }
        for (GqlStatusInfoCodes gqlCode : GqlStatusInfoCodes.values()) {
            final var keys = gqlCode.getStatusParameterKeys();
            final var keySet = EnumSet.noneOf(GqlMessageParams.class);
            keySet.addAll(keys);
            assertThat(gqlCode.parameterCount())
                    .describedAs("Number of parameters needs to match the message template")
                    .isEqualTo(gqlCode.messageFormatParameterCount());

            assertThat(keys)
                    .allSatisfy(key -> assertThat(key.name())
                            .describedAs("Parameters must be a camelCase word (possibly containing numbers)")
                            .matches("^[a-z][a-zA-Z0-9]*$"))
                    .hasSize(gqlCode.parameterCount());

            if (!keys.isEmpty()) {
                assertThat(gqlCode.getMessage(allUniqueParams))
                        .describedAs("Message should contain all expected parameters")
                        .contains(filterValues(allUniqueParams, keySet::contains));
                assertThat(gqlCode.getMessage(orderKeys(allUniqueParams, keys)))
                        .describedAs("Message should contain all expected parameters")
                        .contains(filterValues(allUniqueParams, keySet::contains));
            }

            assertThat(gqlCode.getMessage(allUniqueParams))
                    .describedAs("Message should not contain unexpected parameters")
                    .doesNotContain(filterValues(allUniqueParams, k -> !keySet.contains(k)));
            assertThat(gqlCode.getMessage(orderKeys(allUniqueParams, keys)))
                    .describedAs("Message should not contain unexpected parameters")
                    .doesNotContain(filterValues(allUniqueParams, k -> !keySet.contains(k)));
        }
    }

    @Test
    void verifySubConditionStartsWithLowerCase() {
        Set<GqlStatusInfoCodes> whitelist = new HashSet<>();
        whitelist.add(GqlStatusInfoCodes.STATUS_52N25);
        whitelist.add(GqlStatusInfoCodes.STATUS_22N49);
        whitelist.add(GqlStatusInfoCodes.STATUS_51N09);
        whitelist.add(GqlStatusInfoCodes.STATUS_51N68);
        for (GqlStatusInfoCodes gqlCode : GqlStatusInfoCodes.values()) {
            var subcond = gqlCode.getSubCondition();
            if (!subcond.isEmpty()) {
                var firstChar = subcond.charAt(0);
                var isLowerCase = Character.isLowerCase(firstChar);
                var isWhitelisted = whitelist.contains(gqlCode);
                if (isLowerCase && isWhitelisted) {
                    // If it's whitelisted but it's not needed, please remove it
                    fail("Subcondition for " + gqlCode + " doesn't need to be whitelisted");
                } else if (!isLowerCase && !isWhitelisted) {
                    fail(gqlCode + " has subcondition not starting with lowercase");
                }
            }
        }
    }

    @Test
    void verifySubConditionNotEndingInFullStop() {
        for (GqlStatusInfoCodes gqlCode : GqlStatusInfoCodes.values()) {
            var subcond = gqlCode.getSubCondition();
            if (!subcond.isEmpty()) {
                var lastChar = subcond.charAt(subcond.length() - 1);
                if (String.valueOf(lastChar).matches("[.!?]")) {
                    fail(gqlCode + " has subcondition ending in a full stop");
                }
            }
        }
    }

    @Test
    void verifyMessageEndsWithFullStop() {
        Set<GqlStatusInfoCodes> whitelist = new HashSet<>();
        whitelist.add(GqlStatusInfoCodes.STATUS_01N00);
        whitelist.add(GqlStatusInfoCodes.STATUS_42N06);
        whitelist.add(GqlStatusInfoCodes.STATUS_42I45);
        whitelist.add(GqlStatusInfoCodes.STATUS_42N72);
        whitelist.add(GqlStatusInfoCodes.STATUS_42N89);
        whitelist.add(GqlStatusInfoCodes.STATUS_50N00);
        whitelist.add(GqlStatusInfoCodes.STATUS_50N01);
        whitelist.add(GqlStatusInfoCodes.STATUS_51N54);
        whitelist.add(GqlStatusInfoCodes.STATUS_52U00);
        for (GqlStatusInfoCodes gqlCode : GqlStatusInfoCodes.values()) {
            var message = gqlCode.getMessage(Map.of());
            if (!message.isEmpty()) {
                var lastChar = message.charAt(message.length() - 1);
                var endsWithFullStop = String.valueOf(lastChar).matches("[.!?]");
                if (endsWithFullStop && whitelist.contains(gqlCode)) {
                    // If it's whitelisted but it's not needed, please remove it
                    fail("Message for " + gqlCode + " doesn't need to be whitelisted");
                }
                if (!endsWithFullStop && !whitelist.contains(gqlCode)) {
                    fail(gqlCode + " has message not ending in a full stop");
                }
            }
        }
    }

    @Test
    void verifyMessageStartsWithUpperCaseOrParamOrQuery() {
        Set<GqlStatusInfoCodes> whitelist = new HashSet<>();
        for (GqlStatusInfoCodes gqlCode : GqlStatusInfoCodes.values()) {
            var message = gqlCode.getMessage(new Object[] {"A"});
            if (!message.isEmpty()) {
                var firstChar = message.charAt(0);
                var startsWithUpperCaseOrParam = String.valueOf(firstChar).matches("^[`$A-Z]");
                if (startsWithUpperCaseOrParam && whitelist.contains(gqlCode)) {
                    // If it's whitelisted but it's not needed, please remove it
                    fail("Message for " + gqlCode + " doesn't need to be whitelisted");
                }
                if (!startsWithUpperCaseOrParam && !whitelist.contains(gqlCode)) {
                    fail(gqlCode + " has message not starting with uppercase, parameter or query");
                }
            }
        }
    }

    @Test
    void verifyEnumNameMatchesStatusString() {
        for (var gqlCode : GqlStatusInfoCodes.values()) {
            var enumName = gqlCode.name();
            var statusString = gqlCode.getStatusString();
            if (!enumName.matches("STATUS_[A-Z0-9]{5}")) {
                fail("the enum name for " + gqlCode + " doesn't match the expected format");
            }
            var subString = enumName.substring(7); // at index 8 the actual code starts
            if (!subString.equals(statusString)) {
                fail(gqlCode + " the enum name and the given status string doesn't match");
            }
        }
    }

    @Test
    void verifyEnumsComeInAlphabeticalOrder() {
        var sorted = new ArrayList<>(asList(GqlStatusInfoCodes.values()));
        sorted.sort(Comparator.comparing(GqlStatusInfoCodes::getStatusString));
        var declared = List.of(GqlStatusInfoCodes.values());
        if (!sorted.equals(declared)) {
            fail("Please make sure that the GqlCode enums are in sorted order");
        }
    }

    @Test
    void verifyMessageIsNotOnlyWhitespace() {
        for (var gqlCode : GqlStatusInfoCodes.values()) {
            var message = gqlCode.getMessage(Map.of());
            if (!message.isEmpty() && message.matches("\\s*")) {
                fail("The message for " + gqlCode + " is non-empty but contains only whitespaces");
            }
        }
    }

    @Test
    void verifyGqlStatusHaveNotChanged() {
        final var params = new EnumMap<>(GqlMessageParams.class);
        for (final var p : GqlMessageParams.values()) params.put(p, p.toParamFormat());
        StringBuilder gqlBuilder = new StringBuilder();
        Arrays.stream(GqlStatusInfoCodes.values()).forEach(gqlCode -> {
            gqlBuilder.append(gqlCode.getStatusString());
            gqlBuilder.append(gqlCode.getCondition());
            gqlBuilder.append(gqlCode.getSubCondition());
            gqlBuilder.append(gqlCode.getMessage(params));
            gqlBuilder.append(Arrays.toString(
                    gqlCode.getStatusParameterKeys().stream().map(Enum::name).toArray()));
        });

        byte[] gqlHash = DigestUtils.sha256(gqlBuilder.toString());

        byte[] expectedHash = new byte[] {
            -90, 92, -88, 67, 55, 21, 45, -29, -34, -103, -38, -88, 35, 19, 93, -42, 125, 16, -16, 3, 93, 1, -27, -76,
            54, -57, 13, 70, 20, -48, -37, 2
        };

        if (!Arrays.equals(gqlHash, expectedHash)) {
            Assertions.fail(
                    """
            Expected: %s
            Actual: %s
            Updating the GQL status code is a breaking change!!!
            If parameters are updated, you must change them everywhere they are used (i.e. each time they are used in the call `.withParam(..., ...)`)
            If you update an error message, it is not breaking, but please update documentation.
            """
                            .formatted(Arrays.toString(expectedHash), Arrays.toString(gqlHash)));
        }
    }

    private static Collection<String> filterValues(
            Map<GqlMessageParams, Object> source, Predicate<GqlMessageParams> predicate) {
        return source.entrySet().stream()
                .filter(e -> predicate.test(e.getKey()))
                .map(e -> e.getValue().toString())
                .toList();
    }

    private static Object[] orderKeys(Map<GqlMessageParams, Object> source, List<GqlMessageParams> keep) {
        final var result = new ArrayList<>();
        for (final var p : keep) result.add(source.get(p));
        return result.toArray();
    }
}
