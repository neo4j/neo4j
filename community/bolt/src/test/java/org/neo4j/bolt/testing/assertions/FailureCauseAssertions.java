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
import org.neo4j.gqlstatus.GqlStatus;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;

public final class FailureCauseAssertions extends AbstractGqlMetadataAssertionBuilder<FailureCauseAssertions> {

    private FailureCauseAssertions() {}

    private FailureCauseAssertions(Map<String, List<Assertion>> assertions, boolean lenient) {
        super(assertions, lenient);
    }

    public static FailureCauseAssertions create() {
        return new FailureCauseAssertions();
    }

    @Override
    protected FailureCauseAssertions create(Map<String, List<Assertion>> assertions, boolean lenient) {
        return new FailureCauseAssertions(assertions, lenient);
    }

    /**
     * @deprecated Do not use this method unless you _really_ know what you're doing.
     */
    @Override
    @Deprecated
    public FailureCauseAssertions hasStatus(GqlStatus expected) {
        return super.hasStatus(expected);
    }

    @Override
    public FailureCauseAssertions hasStatus(GqlStatusInfoCodes expected, GqlMessageParameters parameters) {
        return super.hasStatus(expected, parameters);
    }

    @Override
    public FailureCauseAssertions hasStatus(GqlStatusInfoCodes expected) {
        return super.hasStatus(expected);
    }

    @Override
    protected FailureCauseAssertions hasGqlMessage(String expected) {
        return this.registerAssertion(
                MESSAGE_KEY, actual -> Assertions.assertThat(actual).isEqualTo(expected));
    }
}
