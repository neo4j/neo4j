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
package org.neo4j.procedure.builtin.graphschema;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.neo4j.cypher.internal.CypherVersion;
import org.neo4j.cypher.internal.util.UnicodeHelper;

/**
 * This utility class is more or less copy&pasted from Cypher-DSL. Find the original here:
 * <a href="https://github.com/neo4j-contrib/cypher-dsl/tree/main/neo4j-cypher-dsl-schema-name-support">neo4j-cypher-dsl-schema-name-support</a>.
 *
 * All code supporting older versions has been removed.
 */
final class SchemaNames {

    private static final String ESCAPED_UNICODE_BACKTICK = "\\u0060";

    private static final Pattern PATTERN_ESCAPED_4DIGIT_UNICODE = Pattern.compile("\\\\u+(\\p{XDigit}{4})");
    private static final Pattern PATTERN_LABEL_AND_TYPE_QUOTATION = Pattern.compile("(?<!`)`(?:`{2})*(?!`)");

    private static final List<String[]> SUPPORTED_ESCAPE_CHARS = List.of(
            new String[] {"\\b", "\b"},
            new String[] {"\\f", "\f"},
            new String[] {"\\n", "\n"},
            new String[] {"\\r", "\r"},
            new String[] {"\\t", "\t"},
            new String[] {"\\`", "``"});

    private static final int CACHE_SIZE = 128;

    private record SchemaName(String value, boolean needsQuotation) {}

    /**
     * Cypher-DSL has a concrete implementation of such a simple cache, but it should be in this module itself.
     */
    private static final Map<String, SchemaName> CACHE =
            Collections.synchronizedMap(new LinkedHashMap<>(CACHE_SIZE / 4, 0.75f, true) {
                private static final long serialVersionUID = -8109893585632797360L;

                @Override
                protected boolean removeEldestEntry(Map.Entry<String, SchemaName> eldest) {
                    return size() >= CACHE_SIZE;
                }
            });

    /**
     * Sanitizes the given input to be used as a valid schema name, adds quotes if necessary
     *
     * @param value The value to sanitize
     * @return A value that is safe to be used in string concatenation, an empty optional indicates a value that cannot be safely quoted
     */
    public static Optional<String> sanitize(String value) {
        return sanitize(value, false);
    }

    /**
     * Sanitizes the given input to be used as a valid schema name
     *
     * @param value         The value to sanitize
     * @param enforceQuotes If quotation should be enforced, even when not necessary
     * @return A value that is safe to be used in string concatenation, an empty optional indicates a value that cannot be safely quoted
     */
    public static Optional<String> sanitize(String value, boolean enforceQuotes) {

        if (value == null || value.isEmpty()) {
            return Optional.empty();
        }

        SchemaName escapedValue = CACHE.computeIfAbsent(value, SchemaNames::sanitize0);

        if (!(enforceQuotes || escapedValue.needsQuotation)) {
            return Optional.of(escapedValue.value);
        }

        return Optional.of(String.format(Locale.ENGLISH, "`%s`", escapedValue.value));
    }

    private static SchemaName sanitize0(String key) {

        String workingValue = key;

        // Replace current and future escaped chars
        for (String[] pair : SUPPORTED_ESCAPE_CHARS) {
            workingValue = workingValue.replace(pair[0], pair[1]);
        }
        workingValue = workingValue.replace(ESCAPED_UNICODE_BACKTICK, "`");

        // Replace escaped octal hex
        // Excluding the support for 6 digit literals, as this contradicts the overall example in CIP-59r
        Matcher matcher = PATTERN_ESCAPED_4DIGIT_UNICODE.matcher(workingValue);
        StringBuilder sb = new StringBuilder();
        while (matcher.find()) {
            String replacement = Character.toString((char) Integer.parseInt(matcher.group(1), 16));
            matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(sb);
        workingValue = sb.toString();

        workingValue = workingValue.replace("\\u", "\\u005C\\u0075");

        matcher = PATTERN_LABEL_AND_TYPE_QUOTATION.matcher(workingValue);
        workingValue = matcher.replaceAll("`$0").replace("\\\\", "\\");

        return new SchemaName(workingValue, !isIdentifier(workingValue));
    }

    /**
     * This is a literal copy of {@code javax.lang.model.SourceVersion#isIdentifier(CharSequence)} included here to
     * be not dependent on the compiler module.
     *
     * This defaults to Cypher6 as it is safer to quote deprecated unicodes in Cypher5 already.
     * E.g \u0085 is deprecated in Cypher5, and removed in Cypher6, this function will therefore return false if
     * an identifier contains that character, resulting in the identifier being quoted in all versions.
     *
     * @param name A possible Java identifier
     * @return True, if {@code name} represents an identifier.
     */
    private static boolean isIdentifier(CharSequence name) {

        String id = name.toString();
        int cp = id.codePointAt(0);
        if (!UnicodeHelper.isIdentifierStart(cp, CypherVersion.Cypher6)) {
            return false;
        }
        for (int i = Character.charCount(cp); i < id.length(); i += Character.charCount(cp)) {
            cp = id.codePointAt(i);
            if (!UnicodeHelper.isIdentifierPart(cp, CypherVersion.Cypher6)) {
                return false;
            }
        }
        return true;
    }

    private SchemaNames() {}
}
