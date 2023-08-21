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
package org.neo4j.graphdb.schema;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.test.extension.DbmsExtension;

@DbmsExtension
public class CreateConstraintFromToStringCommunityIT extends CreateConstraintFromToStringITBase {
    @ParameterizedTest
    @EnumSource(value = CommunityConstraint.class)
    void shouldRecreateSimilarConstraintFromToStringMethod(CommunityConstraint constraintFunction) {
        testShouldRecreateSimilarConstraintFromToStringMethod(constraintFunction);
    }

    private enum CommunityConstraint implements ConstraintFunction {
        UNIQUE_SINGLE_PROP(schema -> schema.constraintFor(LABEL).assertPropertyIsUnique(PROP_ONE)),
        UNIQUE_MULTI_PROP(schema ->
                schema.constraintFor(LABEL).assertPropertyIsUnique(PROP_ONE).assertPropertyIsUnique(PROP_TWO)),
        REL_UNIQUE_SINGLE_PROP(schema -> schema.constraintFor(REL_TYPE).assertPropertyIsUnique(PROP_ONE)),
        REL_UNIQUE_MULTI_PROP(schema ->
                schema.constraintFor(REL_TYPE).assertPropertyIsUnique(PROP_ONE).assertPropertyIsUnique(PROP_TWO));

        private final ConstraintFunction constraintFunction;

        CommunityConstraint(ConstraintFunction constraintFunction) {
            this.constraintFunction = constraintFunction;
        }

        @Override
        public ConstraintCreator apply(Schema schema) {
            return constraintFunction.apply(schema);
        }
    }
}
