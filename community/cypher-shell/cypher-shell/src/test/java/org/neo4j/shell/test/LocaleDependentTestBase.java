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
package org.neo4j.shell.test;

import java.util.Locale;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/**
 * A helper class to mark tests as locale dependent. It has been introduced to capture / assert a couple of locale dependent tests. Alternatives that have been
 * considered
 * <ul>
 *     <li>Add corresponding configuration to the local `pom.xml` (discarded because it would require also configuration of forking etc.)</li>
 *     <li>SystemLambda to set system properties (discarded, as the default locale won't change on changed sys properties)</li>
 *     <li>Locally inside failing tests (discarded, duplicate work, will fail the next test)</li>
 * </ul>
 */
public abstract class LocaleDependentTestBase {
    private static Locale OLD_DEFAULT_LOCALE;

    @BeforeAll
    static void enforceEnglishLocale() {
        OLD_DEFAULT_LOCALE = Locale.getDefault();
        Locale.setDefault(Locale.ENGLISH);
    }

    @AfterAll
    static void restoreDefaultLocale() {
        if (OLD_DEFAULT_LOCALE == null) {
            return;
        }
        Locale.setDefault(OLD_DEFAULT_LOCALE);
    }
}
