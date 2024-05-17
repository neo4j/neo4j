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
package org.neo4j.internal.kernel.api.procs;

import static java.util.Collections.unmodifiableList;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_STRING_ARRAY;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.api.CypherScope;

/**
 * This describes the signature of a function, made up of its namespace, name, and input/output description.
 * Function uniqueness is currently *only* on the namespace/name level - no function overloading allowed (yet).
 * <p>
 * Note: These constructors are used by APOC-extended, and a gradual deprecation strategy should therefore be used.
 * </p>
 */
public final class UserFunctionSignature {
    private final QualifiedName name;
    private final List<FieldSignature> inputSignature;
    private final Neo4jTypes.AnyType type;
    private final boolean isDeprecated;
    private final String deprecated;
    private final String description;
    private final String category;
    private final boolean caseInsensitive;
    private final boolean isBuiltIn;
    private final boolean internal;
    private final boolean threadSafe;
    private final Set<CypherScope> supportedCypherScopes;

    @Deprecated(forRemoval = true)
    @SuppressWarnings("unused")
    public UserFunctionSignature(
            QualifiedName name,
            List<FieldSignature> inputSignature,
            Neo4jTypes.AnyType type,
            String deprecated,
            String description,
            String category,
            boolean caseInsensitive,
            boolean isBuiltIn,
            boolean internal,
            boolean threadSafe) {
        this(
                name,
                inputSignature,
                type,
                deprecated != null && !deprecated.isEmpty(),
                deprecated,
                description,
                category,
                caseInsensitive,
                isBuiltIn,
                internal,
                threadSafe,
                CypherScope.ALL_SCOPES);
    }

    @Deprecated(forRemoval = true)
    public UserFunctionSignature(
            QualifiedName name,
            List<FieldSignature> inputSignature,
            Neo4jTypes.AnyType type,
            boolean isDeprecated,
            String deprecatedBy,
            String description,
            String category,
            boolean caseInsensitive,
            boolean isBuiltIn,
            boolean internal,
            boolean threadSafe) {
        this(
                name,
                inputSignature,
                type,
                isDeprecated,
                deprecatedBy,
                description,
                category,
                caseInsensitive,
                isBuiltIn,
                internal,
                threadSafe,
                CypherScope.ALL_SCOPES);
    }

    public UserFunctionSignature(
            QualifiedName name,
            List<FieldSignature> inputSignature,
            Neo4jTypes.AnyType type,
            boolean isDeprecated,
            String deprecatedBy,
            String description,
            String category,
            boolean caseInsensitive,
            boolean isBuiltIn,
            boolean internal,
            boolean threadSafe,
            Set<CypherScope> supportedCypherScopes) {
        this.name = name;
        this.inputSignature = unmodifiableList(inputSignature);
        this.type = type;
        this.isDeprecated = isDeprecated || (deprecatedBy != null && !deprecatedBy.isEmpty());
        this.deprecated = deprecatedBy;
        this.description = description;
        this.category = category;
        this.caseInsensitive = caseInsensitive;
        this.isBuiltIn = isBuiltIn;
        this.internal = internal;
        this.threadSafe = threadSafe;
        this.supportedCypherScopes = supportedCypherScopes;
    }

    public QualifiedName name() {
        return name;
    }

    public boolean isDeprecated() {
        return isDeprecated;
    }

    public Optional<String> deprecated() {
        return Optional.ofNullable(deprecated);
    }

    public List<FieldSignature> inputSignature() {
        return inputSignature;
    }

    public Neo4jTypes.AnyType outputType() {
        return type;
    }

    public Optional<String> description() {
        return Optional.ofNullable(description);
    }

    public Optional<String> category() {
        return Optional.ofNullable(category);
    }

    public boolean caseInsensitive() {
        return caseInsensitive;
    }

    public boolean isBuiltIn() {
        return isBuiltIn;
    }

    public boolean internal() {
        return internal;
    }

    public boolean threadSafe() {
        return threadSafe;
    }

    public Set<CypherScope> supportedCypherScopes() {
        return supportedCypherScopes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        UserFunctionSignature that = (UserFunctionSignature) o;
        return name.equals(that.name) && inputSignature.equals(that.inputSignature) && type.equals(that.type);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public String toString() {
        String strInSig = inputSignature == null ? "..." : Iterables.toString(inputSignature, ", ");
        String strOutSig = type == null ? "..." : type.toString();
        return String.format("%s(%s) :: %s", name, strInSig, strOutSig);
    }

    public static class Builder {
        private final QualifiedName name;
        private final List<FieldSignature> inputSignature = new LinkedList<>();
        private Neo4jTypes.AnyType outputType;
        private boolean isDeprecated = false;
        private String deprecated;
        private String description;
        private String category;
        private boolean threadSafe;
        private Set<CypherScope> supportedCypherScopes = CypherScope.ALL_SCOPES;

        public Builder(String[] namespace, String name) {
            this.name = new QualifiedName(namespace, name);
        }

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public Builder category(boolean isDeprecated) {
            this.isDeprecated = isDeprecated;
            return this;
        }

        public Builder deprecated(String deprecated) {
            this.isDeprecated = deprecated != null && !deprecated.isEmpty();
            this.deprecated = deprecated;
            return this;
        }

        public Builder deprecatedBy(String deprecated) {
            this.deprecated = deprecated;
            return this;
        }

        /** Define an input field */
        public Builder in(String name, Neo4jTypes.AnyType type) {
            inputSignature.add(FieldSignature.inputField(name, type));
            return this;
        }

        public Builder in(String name, Neo4jTypes.AnyType type, DefaultParameterValue defaultValue) {
            inputSignature.add(FieldSignature.inputField(name, type, defaultValue));
            return this;
        }

        /** Define an output field */
        public Builder out(Neo4jTypes.AnyType type) {
            outputType = type;
            return this;
        }

        public Builder threadSafe() {
            this.threadSafe = true;
            return this;
        }

        public Builder supportedCypherScopes(CypherScope... versions) {
            this.supportedCypherScopes = EnumSet.copyOf(Arrays.asList(versions));
            return this;
        }

        public UserFunctionSignature build() {
            if (outputType == null) {
                throw new IllegalStateException("output type must be set");
            }
            return new UserFunctionSignature(
                    name,
                    inputSignature,
                    outputType,
                    isDeprecated || (deprecated != null && !deprecated.isEmpty()),
                    deprecated,
                    description,
                    category,
                    false,
                    false,
                    false,
                    threadSafe,
                    supportedCypherScopes);
        }
    }

    public static Builder functionSignature(String... namespaceAndName) {
        String[] namespace = namespaceAndName.length > 1
                ? Arrays.copyOf(namespaceAndName, namespaceAndName.length - 1)
                : EMPTY_STRING_ARRAY;
        String name = namespaceAndName[namespaceAndName.length - 1];
        return functionSignature(namespace, name);
    }

    public static Builder functionSignature(QualifiedName name) {
        return new Builder(name.namespace(), name.name());
    }

    public static Builder functionSignature(String[] namespace, String name) {
        return new Builder(namespace, name);
    }
}
