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
package org.neo4j.internal.kernel.api.security;

import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.TRUE;

import java.util.function.BiPredicate;
import java.util.function.Predicate;
import org.neo4j.util.Preconditions;
import org.neo4j.values.storable.Value;
import org.neo4j.values.utils.ValueBooleanLogic;

public interface PropertyRule extends Predicate<Value> {

    static PropertyRule newRule(int propertyKey, Value value, ComparisonOperator operator) {
        return new PropertyRule.PropertyValueRule(propertyKey, value, operator);
    }

    static PropertyRule newNullRule(int propertyKey, NullOperator operator) {
        return new PropertyRule.NullPropertyRule(propertyKey, operator);
    }

    int property();

    enum ComparisonOperator implements BiPredicate<Value, Value> {
        GREATER_THAN(">") {
            @Override
            public boolean test(Value lhs, Value rhs) {
                return ValueBooleanLogic.greaterThan(lhs, rhs).equals(TRUE);
            }
        },
        LESS_THAN("<") {
            @Override
            public boolean test(Value lhs, Value rhs) {
                return ValueBooleanLogic.lessThan(lhs, rhs).equals(TRUE);
            }
        },
        GREATER_THAN_OR_EQUAL(">=") {
            @Override
            public boolean test(Value lhs, Value rhs) {
                return ValueBooleanLogic.greaterThanOrEqual(lhs, rhs).equals(TRUE);
            }
        },
        LESS_THAN_OR_EQUAL("<=") {
            @Override
            public boolean test(Value lhs, Value rhs) {
                return ValueBooleanLogic.lessThanOrEqual(lhs, rhs).equals(TRUE);
            }
        },
        EQUAL("=") {
            @Override
            public boolean test(Value lhs, Value rhs) {
                return ValueBooleanLogic.equals(lhs, rhs).equals(TRUE);
            }
        },
        NOT_EQUAL("<>") {
            @Override
            public boolean test(Value lhs, Value rhs) {
                return ValueBooleanLogic.notEquals(lhs, rhs).equals(TRUE);
            }
        },
        IN("IN") {
            @Override
            public boolean test(Value lhs, Value rhs) {
                return ValueBooleanLogic.in(lhs, rhs).equals(TRUE);
            }
        },
        NOT_IN("NOT IN") {
            @Override
            public boolean test(Value lhs, Value rhs) {
                return ValueBooleanLogic.not(ValueBooleanLogic.in(lhs, rhs)).equals(TRUE);
            }

            @Override
            public String toPredicateString(String lhs, String rhs) {
                return String.format("NOT %s IN %s", lhs, rhs);
            }
        };

        private final String symbol;

        ComparisonOperator(String symbol) {
            this.symbol = symbol;
        }

        public String getSymbol() {
            return symbol;
        }

        public String toPredicateString(String lhs, String rhs) {
            return String.format("%s %s %s", lhs, getSymbol(), rhs);
        }
    }

    enum NullOperator implements Predicate<Value> {
        IS_NULL("IS NULL") {
            @Override
            public boolean test(Value lhs) {
                return lhs == NO_VALUE;
            }
        },
        IS_NOT_NULL("IS NOT NULL") {
            @Override
            public boolean test(Value lhs) {
                return lhs != NO_VALUE;
            }
        };

        private final String symbol;

        NullOperator(String symbol) {
            this.symbol = symbol;
        }

        public String getSymbol() {
            return symbol;
        }

        public String toPredicateString(String lhs) {
            return String.format("%s %s", lhs, getSymbol());
        }
    }

    record PropertyValueRule(int property, Value value, ComparisonOperator operator) implements PropertyRule {

        public PropertyValueRule {
            Preconditions.requireNonNull(value, "value must not be null");
        }

        @Override
        public boolean test(Value value) {
            return operator.test(value, this.value);
        }
    }

    record NullPropertyRule(int property, NullOperator operator) implements PropertyRule {
        @Override
        public boolean test(Value value) {
            return operator.test(value);
        }
    }
}
