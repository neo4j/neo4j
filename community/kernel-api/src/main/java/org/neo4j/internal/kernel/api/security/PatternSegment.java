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

import java.util.Set;
import java.util.stream.Collectors;
import org.neo4j.internal.helpers.NameUtil;
import org.neo4j.util.Preconditions;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public interface PatternSegment extends Segment {

    default String elementTypeString() {
        return elementTypes().isEmpty()
                ? ""
                : elementTypes().stream().sorted().map(NameUtil::escapeName).collect(Collectors.joining("|", ":", ""));
    }

    default String propertyString(String elementVariableName) {
        return String.format("%s.%s", elementVariableName, NameUtil.escapeName(property()));
    }

    default String prettyPrintValue(Value value) {
        String method = null;
        String prettyPrintedValue = value.prettyPrint();

        switch (value) {
            case DateValue ignored -> method = "date";
            case LocalDateTimeValue ignored -> method = "localdatetime";
            case DateTimeValue ignored -> method = "datetime";
            case LocalTimeValue ignored -> method = "localtime";
            case TimeValue ignored -> method = "time";
            case DurationValue ignored -> method = "duration";
            case PointValue ignored -> prettyPrintedValue = value.toString();
            default -> {}
        }
        return method == null ? prettyPrintedValue : String.format("%s('%s')", method, prettyPrintedValue);
    }

    PropertyRule buildPropertyRule(int securityProperty);

    Set<String> ALL_ELEMENT_TYPES = Set.of();

    Set<String> elementTypes();

    String property();

    String pattern();

    @Override
    default boolean satisfies(Segment segment) {
        throw new UnsupportedOperationException();
    }

    @Override
    default String toCypherSnippet() {
        return String.format("FOR %s", pattern());
    }

    abstract class ValuePatternSegment implements PatternSegment {
        protected final Set<String> elementTypes;
        protected final String property;
        protected final Value value;
        protected final PropertyRule.ComparisonOperator operator;

        public ValuePatternSegment(
                Set<String> elementTypes, String property, Value value, PropertyRule.ComparisonOperator operator) {
            Preconditions.requireNonNull(elementTypes, "elementTypes must not be null");
            Preconditions.requireNonNull(property, "property must not be null");
            Preconditions.requireNonNull(value, "value must not be null");
            Preconditions.checkArgument(
                    value != Values.NO_VALUE, "value must not be NO_VALUE. Use NullPatternSegment for this purpose.");
            this.elementTypes = elementTypes;
            this.property = property;
            this.value = value;
            this.operator = operator;
        }

        @Override
        public Set<String> elementTypes() {
            return elementTypes;
        }

        @Override
        public String property() {
            return property;
        }

        public Value value() {
            return value;
        }

        public PropertyRule.ComparisonOperator operator() {
            return operator;
        }

        @Override
        public PropertyRule buildPropertyRule(int securityProperty) {
            return PropertyRule.newRule(securityProperty, value(), operator());
        }

        @Override
        public String toString() {
            return String.format("FOR(%s)", pattern());
        }
    }

    class NodeValuePatternSegment extends ValuePatternSegment {
        public NodeValuePatternSegment(
                Set<String> elementTypes, String property, Value value, PropertyRule.ComparisonOperator operator) {
            super(elementTypes, property, value, operator);
        }

        public NodeValuePatternSegment(String property, Value value, PropertyRule.ComparisonOperator operator) {
            this(ALL_ELEMENT_TYPES, property, value, operator);
        }

        @Override
        public String pattern() {
            return String.format(
                    "(n%s) WHERE %s",
                    elementTypeString(),
                    this.operator.toPredicateString(propertyString("n"), prettyPrintValue(this.value)));
        }
    }

    class RelValuePatternSegment extends ValuePatternSegment {
        public RelValuePatternSegment(
                Set<String> elementTypes, String property, Value value, PropertyRule.ComparisonOperator operator) {
            super(elementTypes, property, value, operator);
        }

        public RelValuePatternSegment(String property, Value value, PropertyRule.ComparisonOperator operator) {
            this(ALL_ELEMENT_TYPES, property, value, operator);
        }

        @Override
        public String pattern() {
            return String.format(
                    "()-[r%s]-() WHERE %s",
                    elementTypeString(),
                    this.operator.toPredicateString(propertyString("r"), prettyPrintValue(this.value)));
        }
    }

    /**
     * A pattern segment whose comparison value comes from a per-user ABAC function (today
     * {@code abac.native.user_tags()}) rather than a literal. The function cannot be evaluated when the
     * privilege is granted -- there is no user in context -- so only its name is stored, and the value is
     * supplied per user via {@link #buildPropertyRule(int, Value)} when the access mode is built. The segment
     * names the function without knowing which provider backs it, so a further ABAC function needs no new
     * segment type.
     *
     * <p>Restricted to {@code IN}: it is the operator that takes the list of attributes such a function
     * returns, and negating a per-user list is rejected when the privilege is granted, so no negated form
     * can reach here.
     */
    abstract class AbacPatternSegment implements PatternSegment {
        protected final Set<String> elementTypes;
        protected final String property;
        protected final String functionName;
        protected final PropertyRule.ComparisonOperator operator;

        public AbacPatternSegment(
                Set<String> elementTypes,
                String property,
                String functionName,
                PropertyRule.ComparisonOperator operator) {
            Preconditions.requireNonNull(elementTypes, "elementTypes must not be null");
            Preconditions.requireNonNull(property, "property must not be null");
            Preconditions.requireNonNull(functionName, "functionName must not be null");
            Preconditions.checkArgument(operator == PropertyRule.ComparisonOperator.IN, "operator must be IN");
            this.elementTypes = elementTypes;
            this.property = property;
            this.functionName = functionName;
            this.operator = operator;
        }

        @Override
        public Set<String> elementTypes() {
            return elementTypes;
        }

        @Override
        public String property() {
            return property;
        }

        public String functionName() {
            return functionName;
        }

        public PropertyRule.ComparisonOperator operator() {
            return operator;
        }

        @Override
        public PropertyRule buildPropertyRule(int securityProperty) {
            throw new UnsupportedOperationException(
                    "abac pattern segments are resolved with user context before building property rules");
        }

        public PropertyRule buildPropertyRule(int securityProperty, Value resolvedValue) {
            return PropertyRule.newRule(securityProperty, resolvedValue, operator());
        }

        String predicateString(String elementVariableName) {
            return operator.toPredicateString(propertyString(elementVariableName), functionName + "()");
        }

        @Override
        public String toString() {
            return String.format("FOR(%s)", pattern());
        }
    }

    class NodeAbacPatternSegment extends AbacPatternSegment {
        public NodeAbacPatternSegment(
                Set<String> elementTypes,
                String property,
                String functionName,
                PropertyRule.ComparisonOperator operator) {
            super(elementTypes, property, functionName, operator);
        }

        public NodeAbacPatternSegment(String property, String functionName, PropertyRule.ComparisonOperator operator) {
            this(ALL_ELEMENT_TYPES, property, functionName, operator);
        }

        @Override
        public String pattern() {
            return String.format("(n%s) WHERE %s", elementTypeString(), predicateString("n"));
        }
    }

    class RelAbacPatternSegment extends AbacPatternSegment {
        public RelAbacPatternSegment(
                Set<String> elementTypes,
                String property,
                String functionName,
                PropertyRule.ComparisonOperator operator) {
            super(elementTypes, property, functionName, operator);
        }

        public RelAbacPatternSegment(String property, String functionName, PropertyRule.ComparisonOperator operator) {
            this(ALL_ELEMENT_TYPES, property, functionName, operator);
        }

        @Override
        public String pattern() {
            return String.format("()-[r%s]-() WHERE %s", elementTypeString(), predicateString("r"));
        }
    }

    abstract class NullPatternSegment implements PatternSegment {

        protected final Set<String> elementTypes;
        protected final String property;
        protected final PropertyRule.NullOperator operator;

        public NullPatternSegment(Set<String> elementTypes, String property, PropertyRule.NullOperator operator) {
            Preconditions.requireNonNull(elementTypes, "elementTypes must not be null");
            Preconditions.requireNonNull(property, "property must not be null");
            this.elementTypes = elementTypes;
            this.property = property;
            this.operator = operator;
        }

        @Override
        public Set<String> elementTypes() {
            return elementTypes;
        }

        @Override
        public String property() {
            return property;
        }

        public PropertyRule.NullOperator operator() {
            return operator;
        }

        @Override
        public PropertyRule buildPropertyRule(int securityProperty) {
            return PropertyRule.newNullRule(securityProperty, operator());
        }

        @Override
        public String toString() {
            return String.format("FOR(%s)", pattern());
        }
    }

    class NodeNullPatternSegment extends NullPatternSegment {
        public NodeNullPatternSegment(Set<String> elementTypes, String property, PropertyRule.NullOperator operator) {
            super(elementTypes, property, operator);
        }

        public NodeNullPatternSegment(String property, PropertyRule.NullOperator operator) {
            this(ALL_ELEMENT_TYPES, property, operator);
        }

        @Override
        public String pattern() {
            return String.format(
                    "(n%s) WHERE %s", elementTypeString(), this.operator.toPredicateString(propertyString("n")));
        }
    }

    class RelNullPatternSegment extends NullPatternSegment {
        public RelNullPatternSegment(Set<String> elementTypes, String property, PropertyRule.NullOperator operator) {
            super(elementTypes, property, operator);
        }

        public RelNullPatternSegment(String property, PropertyRule.NullOperator operator) {
            this(ALL_ELEMENT_TYPES, property, operator);
        }

        @Override
        public String pattern() {
            return String.format(
                    "()-[r%s]-() WHERE %s", elementTypeString(), this.operator.toPredicateString(propertyString("r")));
        }
    }
}
