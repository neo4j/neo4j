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

import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.commons.codec.digest.DigestUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class GqlStatusInfoCodesTest {

    private List<String> extractParametersFromMessage(GqlStatusInfoCodes c) {
        String msg = c.getMessage();
        // Find all camelCase words that comes after a $-sign (indicating start of parameter)
        Pattern p = Pattern.compile("\\$([a-z][a-zA-Z0-9]*([0-9]?[A-Z][a-z0-9]*)*)");
        Matcher matcher = p.matcher(msg);
        List<String> parameters = new ArrayList<>();
        while (matcher.find()) {
            String str = matcher.group(1);
            parameters.add(str);
        }
        return parameters;
    }

    @Test
    void verifyParametersCorrectlyWritten() {
        for (GqlStatusInfoCodes gqlCode : GqlStatusInfoCodes.values()) {
            // Parameters must be a camelCase word (possibly containing numbers)
            Stream.of(gqlCode.getStatusParameterKeys()).forEach((key) -> {
                // this $ at the end is regex syntax for end of string, not referring to $ as start of parameter
                if (!key.matches("^[a-z][a-zA-Z0-9]*([0-9]?[A-Z][a-z0-9]*)*$")) {
                    fail("Parameter key `" + key + "` for " + gqlCode + " is not in camel case");
                }
            });
            var declaredParameters = gqlCode.getStatusParameterKeys();
            var parametersInMessage = extractParametersFromMessage(gqlCode);
            // Parameters needs to be the same and declared in the same order
            if (!List.of(declaredParameters).equals(parametersInMessage)) {
                fail(
                        """
                        Parameters used in message for %s does not match the ones declared as parameter keys.
                        Declared: %s
                        In message: %s
                        """
                                .formatted(gqlCode, Arrays.toString(declaredParameters), parametersInMessage));
            }
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
            var message = gqlCode.getMessage();
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
            var message = gqlCode.getMessage();
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
        var sorted = new ArrayList<>(Arrays.asList(GqlStatusInfoCodes.values()));
        sorted.sort(Comparator.comparing(GqlStatusInfoCodes::getStatusString));
        var declared = List.of(GqlStatusInfoCodes.values());
        if (!sorted.equals(declared)) {
            fail("Please make sure that the GqlCode enums are in sorted order");
        }
    }

    @Test
    void verifyMessageIsNotOnlyWhitespace() {
        for (var gqlCode : GqlStatusInfoCodes.values()) {
            var message = gqlCode.getMessage();
            if (!message.isEmpty() && message.matches("\\s*")) {
                fail("The message for " + gqlCode + " is non-empty but contains only whitespaces");
            }
        }
    }

    @Test
    void verifyGqlStatusHaveNotChanged() {
        StringBuilder gqlBuilder = new StringBuilder();
        Arrays.stream(GqlStatusInfoCodes.values()).forEach(gqlCode -> {
            gqlBuilder.append(gqlCode.getStatusString());
            gqlBuilder.append(gqlCode.getCondition());
            gqlBuilder.append(gqlCode.getSubCondition());
            gqlBuilder.append(gqlCode.getMessage());
            gqlBuilder.append(Arrays.toString(gqlCode.getStatusParameterKeys()));
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
}
