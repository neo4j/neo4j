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
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;

class GqlStatusInfoCodesTest {

    @Test
    void verifyParametersCorrectlyWritten() {
        final var allUniqueParams = new HashMap<GqlParams.GqlParam, Object>();
        Reflections reflections = new Reflections("org.neo4j.gqlstatus", new SubTypesScanner(false));
        Set<Class<? extends GqlParams.GqlParam>> enums = reflections.getSubTypesOf(GqlParams.GqlParam.class);
        int veryUniqueNumber = 34567894;
        for (Class<? extends GqlParams.GqlParam> e : enums) {
            if (e.isEnum()) {
                for (final var p : e.getEnumConstants())
                    if (GqlParams.StringParam.class == e) {
                        allUniqueParams.put(p, p.process("⚠️very-unique-param-value-%s⚠️".formatted(p.name())));
                    } else if (GqlParams.BooleanParam.class == e) {
                        allUniqueParams.put(
                                p,
                                p.process("⚠️this-should-be-ok-since-boolean-processor-is-StringValueOf-%s⚠️"
                                        .formatted(p.name())));
                    } else if (GqlParams.ListParam.class == e && p instanceof GqlParams.ListParam) {
                        allUniqueParams.put(p, List.of("⚠️very-unique-param-value-%s⚠️".formatted(p.name())));
                    } else if (GqlParams.NumberParam.class == e) {
                        allUniqueParams.put(p, veryUniqueNumber++);
                    } else {
                        fail(
                                "GqlParam-type not expected. If you added a new type, add a way to handle your new parameters above, otherwise something is funny");
                    }
            }
        }

        for (GqlStatusInfoCodes gqlCode : GqlStatusInfoCodes.values()) {
            final var keys = gqlCode.getStatusParameterKeys();
            final var keySet = new HashSet<>(keys);
            assertThat(gqlCode.parameterCount())
                    .describedAs("%s: Number of parameters needs to match the message template", gqlCode.name())
                    .isEqualTo(gqlCode.messageFormatParameterCount());

            assertThat(keys)
                    .allSatisfy(key -> assertThat(key.name())
                            .describedAs(
                                    "%s: Parameters must be a camelCase word (possibly containing numbers)",
                                    gqlCode.name())
                            .matches("^[a-z][a-zA-Z0-9]*$"))
                    .hasSize(gqlCode.parameterCount());

            if (!keys.isEmpty()) {
                assertThat(gqlCode.getMessage(allUniqueParams))
                        .describedAs("%s: Message should contain all expected parameters", gqlCode.name())
                        .contains(filterValues(gqlCode, allUniqueParams, keySet::contains));
                assertThat(gqlCode.getMessage(orderKeys(allUniqueParams, keys)))
                        .describedAs("%s: Message should contain all expected parameters", gqlCode.name())
                        .contains(filterValues(gqlCode, allUniqueParams, keySet::contains));
            }

            assertThat(gqlCode.getMessage(allUniqueParams))
                    .describedAs("%s: Message should not contain unexpected parameters", gqlCode.name())
                    .doesNotContain(filterValues(allUniqueParams, k -> !keySet.contains(k)));
            assertThat(gqlCode.getMessage(orderKeys(allUniqueParams, keys)))
                    .describedAs("%s: Message should not contain unexpected parameters", gqlCode.name())
                    .doesNotContain(filterValues(allUniqueParams, k -> !keySet.contains(k)));
        }
    }

    @Test
    void verifySubConditionStartsWithLowerCase() {
        Set<GqlStatusInfoCodes> whitelist = EnumSet.noneOf(GqlStatusInfoCodes.class);
        whitelist.add(GqlStatusInfoCodes.STATUS_52N25);
        whitelist.add(GqlStatusInfoCodes.STATUS_22N49);
        whitelist.add(GqlStatusInfoCodes.STATUS_51N09);
        whitelist.add(GqlStatusInfoCodes.STATUS_51N68);
        whitelist.add(GqlStatusInfoCodes.STATUS_42N84);
        whitelist.add(GqlStatusInfoCodes.STATUS_01N74);
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
    void verifyParameterMarkersHaveCorrectFormat() {
        for (GqlStatusInfoCodes gqlCode : GqlStatusInfoCodes.values()) {
            var template = gqlCode.getTemplate();
            var numAlmostSubs = gqlCode.getOffsets(template, "%s")
                    .length; // If the substitution-pattern is updated, we will need to change this. Maybe it will be
            // updated to be so clear this test is redundant
            var trueSubs = gqlCode.getOffsets(template, GqlParams.substitution).length;
            if (numAlmostSubs != trueSubs)
                fail(
                        gqlCode.name()
                                + ": Some substitution-patterns are faulty in some GqlStatusInfoCodes template(s), probably with a blankspace too few/many"); // I used this pattern: [^\{].%s.[^\}] to find faulty
        }
    }

    @Test
    void verifyMessageEndsWithFullStopOrParam() {
        Set<GqlStatusInfoCodes> whitelist = EnumSet.noneOf(GqlStatusInfoCodes.class);
        ArrayList<String> dontNeedWhiteList = new ArrayList<>();
        ArrayList<String> badEnding = new ArrayList<>();
        for (GqlStatusInfoCodes gqlCode : GqlStatusInfoCodes.values()) {
            var template = gqlCode.getTemplate();
            if (!template.isEmpty()) {
                var lastChar = template.charAt(template.length() - 1);
                var endsWithFullStop = String.valueOf(lastChar).matches("[.!?]");
                var endsWithParam = template.endsWith(GqlParams.substitution);
                if ((endsWithFullStop || endsWithParam) && whitelist.contains(gqlCode)) {
                    dontNeedWhiteList.add("\n" + gqlCode.name());
                }
                if (!endsWithFullStop && !endsWithParam && !whitelist.contains(gqlCode)) {
                    badEnding.add("\n" + gqlCode.name());
                }
            }
        }
        if (!dontNeedWhiteList.isEmpty()) {
            // If it's whitelisted but it's not needed, please remove it
            fail("Messages for " + dontNeedWhiteList + "don't need to be whitelisted");
        }
        if (!badEnding.isEmpty()) {
            fail(badEnding + "\nhave messages not ending with full stop");
        }
    }

    @Test
    void verifyMessageStartsWithUpperCaseOrParamOrQuery() {
        Set<GqlStatusInfoCodes> whitelist = new HashSet<>();
        for (GqlStatusInfoCodes gqlCode : GqlStatusInfoCodes.values()) {
            var message = gqlCode.getMessage(new Object[] {"A"});
            if (!message.isEmpty()) {
                var firstChar = message.charAt(0);
                var startsWithUpperCaseOrParam = String.valueOf(firstChar).matches("^['`$A-Z]");
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
    void verifySingleWhitespaces() {
        Set<GqlStatusInfoCodes> whitelist = EnumSet.noneOf(GqlStatusInfoCodes.class);
        var regex = "\\s\\s";
        Pattern pattern = Pattern.compile(regex);
        ArrayList<String> dontNeedWhiteList = new ArrayList<>();
        ArrayList<String> multipleWhitespaces = new ArrayList<>();
        for (GqlStatusInfoCodes gqlCode : GqlStatusInfoCodes.values()) {
            var template = gqlCode.getTemplate();
            var subcond = gqlCode.getSubCondition();
            var whiteListed = whitelist.contains(gqlCode);
            var templateHasMultipleWhitespaces = false;
            var subcondHasMultipleWhitespaces = false;
            if (!template.isEmpty()) {
                templateHasMultipleWhitespaces = pattern.matcher(template).find();
            }
            if (!subcond.isEmpty()) {
                subcondHasMultipleWhitespaces = pattern.matcher(subcond).find();
            }
            if (!subcondHasMultipleWhitespaces && !templateHasMultipleWhitespaces && whiteListed) {
                dontNeedWhiteList.add("\n" + gqlCode.name());
            } else if ((subcondHasMultipleWhitespaces || templateHasMultipleWhitespaces) && !whiteListed) {
                multipleWhitespaces.add("\n" + gqlCode.name());
            }
        }
        if (!dontNeedWhiteList.isEmpty()) {
            // If it's whitelisted but it's not needed, please remove it
            fail("Messages for " + dontNeedWhiteList + "don't need to be whitelisted");
        }
        if (!multipleWhitespaces.isEmpty()) {
            fail(multipleWhitespaces + "\nhave messages with multiple whitespaces");
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

    private static class TotalCondition {
        Condition condition;
        String subCondition;

        TotalCondition(Condition condition, String subCondition) {
            this.condition = condition;
            this.subCondition = subCondition;
        }

        @Override
        public int hashCode() {
            return Objects.hash(condition, subCondition);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) return false;
            if (!(obj instanceof TotalCondition tc)) return false;
            else return tc.condition.equals(condition) && tc.subCondition.equals(subCondition);
        }
    }

    @Test
    void verifyConditionSubconditionIsUnique() {
        var whiteList = EnumSet.noneOf(GqlStatusInfoCodes.class);
        whiteList.add(GqlStatusInfoCodes.STATUS_22N12);
        whiteList.add(GqlStatusInfoCodes.STATUS_42NG1);
        var errorMessages = new ArrayList<String>();
        var knownCombinations = new HashMap<TotalCondition, GqlStatusInfoCodes>();
        for (var gqlCode : GqlStatusInfoCodes.values()) {
            if (whiteList.contains(gqlCode)) {
                continue;
            }
            var cond = gqlCode.getCondition();
            var subCond = gqlCode.getSubCondition();
            var tc = new TotalCondition(cond, subCond);
            if (!knownCombinations.containsKey(tc)) {
                knownCombinations.put(tc, gqlCode);
            } else {
                errorMessages.add("\n" + gqlCode + " and " + knownCombinations.get(tc));
            }
        }
        if (!errorMessages.isEmpty()) {
            fail("Condition+SubCondition combinations were not unique:" + errorMessages);
        }
    }

    @Test
    void verifyConditionCorrespondsToUniqueCode() {
        var condCodeMap = new HashMap<String, Condition>();
        for (var gqlCode : GqlStatusInfoCodes.values()) {
            var cond = gqlCode.getCondition();
            var condCode = gqlCode.getGqlStatus().gqlStatusString().substring(0, 2);
            if (!condCodeMap.containsKey(condCode)) {
                condCodeMap.put(condCode, cond);
            } else if (condCodeMap.get(condCode) != cond) {
                fail("The condition for " + gqlCode + " is " + cond + " which is different than "
                        + condCodeMap.get(condCode) + " used elsewhere");
            }
        }
    }

    @Test
    void verifyGetMessageHandlesFaultyParameters() {
        String[] badParam = {"AA", "BBB", "CCC", "DDD", "EEE"};
        for (var gqlCode : GqlStatusInfoCodes.values()) {
            assertThatCode(() -> gqlCode.getMessage((Object[]) badParam))
                    .as("The code %s throws an exception when passed String parameters.", gqlCode)
                    .doesNotThrowAnyException();
        }
    }

    @Test
    void verifyCorrectProcessing() {
        GqlStatusInfoCodes[] gqlCodes = {
            GqlStatusInfoCodes.STATUS_01N02, GqlStatusInfoCodes.STATUS_01N50, GqlStatusInfoCodes.STATUS_03N90
        };
        String[] params = {"Deleting nodes", "Person", "A*B"};
        String[] expectedMessages = {
            "Deleting nodes is deprecated and will be removed without a replacement.",
            // Second parameter $db not sent in so will fallback to print out the parameter name
            "The label `Person` does not exist in database `$db`. Verify that the spelling is correct.",
            "The disconnected pattern 'A*B' builds a cartesian product. A cartesian product may produce a large amount of data and slow down query processing."
        };
        for (int i = 0; i < gqlCodes.length; i++) {
            Object[] param = {params[i]};
            assertThat(gqlCodes[i].getMessage(param))
                    .as("GqlStatusInfoCode %s is incorrectly formatted by getMessage()", gqlCodes[i])
                    .isEqualTo(expectedMessages[i]);
        }
    }

    @Test
    void verifyJoinStyleHasMatchingKey() {
        for (var gqlCode : GqlStatusInfoCodes.values()) {
            if (gqlCode.getJoinStyles() != null) {
                for (var joinStyle : gqlCode.getJoinStyles().keySet()) {
                    assertThat(gqlCode.getStatusParameterKeys())
                            .as("The code %s has JoinStyle key %s but no matching parameter key", gqlCode, joinStyle)
                            .contains(joinStyle);
                }
            }
        }
    }

    @Test
    void verifyJoinStyle() {
        var joinStyledCodes = Arrays.stream(GqlStatusInfoCodes.values())
                .filter(e -> !emptyMap().equals(e.getJoinStyles()))
                .toList();
        String joinWord = ",";
        for (var gqlCode : joinStyledCodes) {
            var statusParameterKeys = gqlCode.getStatusParameterKeys();
            var msgParams = new Object[statusParameterKeys.size()];
            for (int i = 0; i < statusParameterKeys.size(); i++) {
                var paramKey = statusParameterKeys.get(i);
                msgParams[i] = paramKey instanceof GqlParams.ListParam ? List.of("A", "B", "C") : "ABC";
                var joinStyle = gqlCode.getJoinStyles().get(paramKey);
                if (joinStyle != null) {
                    joinWord = switch (joinStyle) {
                        case ANDED -> " and";
                        case ORED -> " or";
                        default -> ",";
                    };
                }
            }
            // Expected here is the String "A, B<joinWord>C" but with any formatting of elements (for now 'A', `A` or
            // $`A`) allowed
            String expected = String.format(
                    "(?:(['`]?)|(\\$)(`))A(\\1|\\3), (\\1|\\2\\3)B(\\1|\\3)%s (\\1|\\2\\3)C(\\1|\\3)",
                    Pattern.quote(joinWord)); // Might need to update the %s here if GqlParams.substitution changes
            Pattern pattern = Pattern.compile(expected);
            var msg = gqlCode.getMessage(msgParams);
            assertThat(msg)
                    .as("The expected list-joinstyle was not inserted into the message string for code %s", gqlCode)
                    .containsPattern(pattern);
        }
    }

    private static Collection<String> filterValues(
            GqlStatusInfoCodes gqlCode,
            Map<GqlParams.GqlParam, Object> source,
            Predicate<GqlParams.GqlParam> predicate) {
        return source.entrySet().stream()
                .filter(e -> predicate.test(e.getKey()))
                .map(e -> checkListProcess(gqlCode, e))
                .toList();
    }

    private static String checkListProcess(GqlStatusInfoCodes gqlCode, Map.Entry<GqlParams.GqlParam, Object> e) {
        if (e.getKey() instanceof GqlParams.ListParam l) {
            if (gqlCode.getJoinStyles() != null) {
                GqlParams.JoinStyle j = gqlCode.getJoinStyles().get(l);
                if (j != null) {
                    return l.process((List<?>) e.getValue(), j);
                }
            }
            return l.process(
                    (List<?>) e.getValue(),
                    GqlParams.JoinStyle.COMMAD); // If COMMAD stops being default this should change
        } else {
            return e.getKey().process(e.getValue());
        }
    }

    private static Collection<String> filterValues(
            Map<GqlParams.GqlParam, Object> source, Predicate<GqlParams.GqlParam> predicate) {
        return source.entrySet().stream()
                .filter(e -> predicate.test(e.getKey()))
                .map(e -> e.getValue().toString())
                .toList();
    }

    private static Object[] orderKeys(Map<GqlParams.GqlParam, Object> source, List<GqlParams.GqlParam> keep) {
        final var result = new ArrayList<>();
        for (final var p : keep) result.add(source.get(p));
        return result.toArray();
    }
}
