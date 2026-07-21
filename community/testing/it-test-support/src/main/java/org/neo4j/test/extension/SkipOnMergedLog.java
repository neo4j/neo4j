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
package org.neo4j.test.extension;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
@SkipTestExtension.Skip(
        key = "NEO4J_OVERRIDE_RAFT_LOG_IMPL",
        values = {"merged", "merged_on_latest"})
public @interface SkipOnMergedLog {
    String reason() default "";

    /**
     * @return an array of {@link Note}, roughly categorizing this skip, or giving hints about why the test is skipped.
     * The enum instances are used to allow finding different types of skips with "Find usages".
     */
    Note[] notes() default {Note.temporary};

    enum Note {
        /**
         * Test is temporarily skipped with the intent to revisit later.
         */
        temporary,
        /**
         * Test and how it's written is fundamentally incompatible with how merged logs works,
         * e.g. being too low-level or something else in the test setup.
         */
        incompatible,
        /**
         * Test has no value for running in a merged logs setup.
         */
        irrelevant,
        /**
         * Test is testing something that isn't supported with merged logs.
         */
        notSupported
    }
}
