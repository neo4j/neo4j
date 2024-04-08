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
package org.neo4j.values.storable;

/**
 * Contains a collection of strings that needs to be supported throughout the product.
 */
class StringsLibrary {
    static final String[] STRINGS = {
        "",
        "1337",
        " ",
        "普通�?/普通話",
        "\uD83D\uDE21",
        "\uD83D\uDE21\uD83D\uDCA9\uD83D\uDC7B",
        " a b c ",
        "䤹᳽",
        "熨",
        "ۼ",
        "ⲹ楡�?톜ഷۢ⼈�?�늉�?�₭샺ጚ砧攡跿家䯶�?⬖�?�犽ۼ",
        " 㺂�?鋦毠", // first character is a thin space,
        "\u0018",
        ";먵�?裬岰鷲趫\uA8C5얱㓙髿ᚳᬼ≩�?� ",
        "\u001cӳ",
        "abcdefghijklmnopqrstuvwxyzåäöABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ 1234567890-´!\"@#$%^&*()_+",
        "йцукенгшщзхъфывапролджэячсмитьбю",
        "abc--hi--abc--abc",
        "𓅀abc--hi--abc--abc𓅀𓅀"
        // TODO longer lorem ipsum string?
    };
}
