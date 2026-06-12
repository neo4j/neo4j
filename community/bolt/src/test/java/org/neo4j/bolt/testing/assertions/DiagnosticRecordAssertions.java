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
package org.neo4j.bolt.testing.assertions;

import java.util.List;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.neo4j.gqlstatus.ErrorClassification;

public final class DiagnosticRecordAssertions extends AbstractMetadataAssertionBuilder<DiagnosticRecordAssertions> {

    private static final String CLASSIFICATION_KEY = "_classification";
    private static final String IDEMPOTENT_KEY = "_idempotent";
    private static final String POSITION_KEY = "_position";
    private static final String POSITION_COLUMN_KEY = "column";
    private static final String POSITION_LINE_KEY = "line";
    private static final String POSITION_OFFSET_KEY = "offset";

    private DiagnosticRecordAssertions() {}

    private DiagnosticRecordAssertions(Map<String, List<Assertion>> assertions, boolean lenient) {
        super(assertions, lenient);
    }

    public static DiagnosticRecordAssertions create() {
        return new DiagnosticRecordAssertions();
    }

    @Override
    protected DiagnosticRecordAssertions create(Map<String, List<Assertion>> assertions, boolean lenient) {
        return new DiagnosticRecordAssertions(assertions, lenient);
    }

    public DiagnosticRecordAssertions hasClassification(ErrorClassification expected) {
        return this.registerAssertion(
                CLASSIFICATION_KEY,
                actual -> Assertions.assertThat(actual).asString().isEqualTo(expected.name()));
    }

    public DiagnosticRecordAssertions hasPosition(long column, long line, long offset) {
        return this.registerAssertion(
                POSITION_KEY,
                actual -> Assertions.assertThat(actual)
                        .asInstanceOf(InstanceOfAssertFactories.map(String.class, Object.class))
                        .containsEntry(POSITION_COLUMN_KEY, column)
                        .containsEntry(POSITION_LINE_KEY, line)
                        .containsEntry(POSITION_OFFSET_KEY, offset)
                        .containsOnlyKeys(POSITION_COLUMN_KEY, POSITION_LINE_KEY, POSITION_OFFSET_KEY));
    }

    public DiagnosticRecordAssertions isIdempotent() {
        return this.registerAssertion(
                IDEMPOTENT_KEY, actual -> Assertions.assertThat(actual).isEqualTo(true));
    }
}
