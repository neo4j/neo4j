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
package org.neo4j.token.api;

import java.util.StringJoiner;
import java.util.function.IntFunction;
import org.neo4j.common.TokenNameLookup;

public final class TokenIdPrettyPrinter {
    private TokenIdPrettyPrinter() {}

    public static String label(int id) {
        return id == TokenConstants.ANY_LABEL ? "" : (":label=" + id);
    }

    public static String propertyKey(int id) {
        return id == TokenConstants.ANY_PROPERTY_KEY ? "" : (":propertyKey=" + id);
    }

    public static String relationshipType(int id) {
        return id == TokenConstants.ANY_RELATIONSHIP_TYPE ? "" : ("[:type=" + id + "]");
    }

    public static String niceQuotedProperties(TokenNameLookup tokenNameLookup, int[] propertyIds) {
        return niceProperties(tokenNameLookup, propertyIds, '(', ')', true);
    }

    public static String niceProperties(TokenNameLookup tokenNameLookup, int[] propertyIds, char prefix, char suffix) {
        return niceProperties(tokenNameLookup, propertyIds, prefix, suffix, false);
    }

    private static String niceProperties(
            TokenNameLookup tokenNameLookup, int[] propertyIds, char prefix, char suffix, boolean alwaysQuote) {
        StringBuilder out = new StringBuilder();
        out.append(prefix);
        format(out, "", ", ", alwaysQuote, tokenNameLookup::propertyKeyGetName, propertyIds);
        out.append(suffix);
        return out.toString();
    }

    public static String niceEntityLabels(IntFunction<String> lookup, int[] labelIds) {
        StringJoiner entityJoiner = new StringJoiner(":", ":", "");
        entityJoiner.setEmptyValue("");
        for (int id : labelIds) {
            String name = lookup.apply(id);
            if (name.contains(":")) {
                name = '`' + name + "`";
            }
            entityJoiner.add(name);
        }
        return entityJoiner.toString();
    }

    public static void format(
            StringBuilder out,
            String prefix,
            String separator,
            boolean alwaysQuote,
            IntFunction<String> lookup,
            int[] ids) {
        for (int id : ids) {
            String name = lookup.apply(id);
            out.append(prefix);
            if (alwaysQuote || name.contains(":")) {
                out.append('`').append(name).append('`');
            } else {
                out.append(name);
            }
            out.append(separator);
        }
        if (ids.length > 0) {
            out.setLength(out.length() - separator.length());
        }
    }
}
