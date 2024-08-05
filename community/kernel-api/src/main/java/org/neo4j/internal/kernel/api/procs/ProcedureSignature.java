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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.api.CypherScope;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.UnsupportedDatabaseTypes;

/**
 * This describes the signature of a procedure, made up of its namespace, name, and input/output description. Procedure uniqueness is currently *only* on the
 * namespace/name level - no procedure overloading allowed (yet).
 * <p>
 * Note: These constructors are used by APOC-extended, and a gradual deprecation strategy should therefore be used.
 * </p>
 */
public class ProcedureSignature {
    public static final List<FieldSignature> VOID = unmodifiableList(new ArrayList<>());
    private final QualifiedName name;
    private final List<FieldSignature> inputSignature;
    private final List<FieldSignature> outputSignature;
    private final Mode mode;
    private final boolean admin;
    private final UnsupportedDatabaseTypes.DatabaseType[] unsupportedDbTypes;
    private final boolean isDeprecated;
    private final String deprecated;
    private final String description;
    private final String warning;
    private final boolean eager;
    private final boolean caseInsensitive;
    private final boolean systemProcedure;
    private final boolean internal;
    private final boolean allowExpiredCredentials;
    private final boolean threadSafe;
    private final Set<CypherScope> supportedCypherScopes;

    @Deprecated(forRemoval = true)
    @SuppressWarnings("unused")
    public ProcedureSignature(
            QualifiedName name,
            List<FieldSignature> inputSignature,
            List<FieldSignature> outputSignature,
            Mode mode,
            boolean admin,
            String deprecated,
            String[] allowed,
            String description,
            String warning,
            boolean eager,
            boolean caseInsensitive,
            boolean systemProcedure,
            boolean internal,
            boolean allowExpiredCredentials) {
        this(
                name,
                inputSignature,
                outputSignature,
                mode,
                admin,
                deprecated != null && !deprecated.isEmpty(),
                deprecated,
                description,
                warning,
                eager,
                caseInsensitive,
                systemProcedure,
                internal,
                allowExpiredCredentials,
                true,
                CypherScope.ALL_SCOPES);
    }

    @Deprecated(forRemoval = true)
    @SuppressWarnings("unused")
    public ProcedureSignature(
            QualifiedName name,
            List<FieldSignature> inputSignature,
            List<FieldSignature> outputSignature,
            Mode mode,
            boolean admin,
            String deprecated,
            String description,
            String warning,
            boolean eager,
            boolean caseInsensitive,
            boolean systemProcedure,
            boolean internal,
            boolean allowExpiredCredentials,
            boolean threadSafe) {
        this(
                name,
                inputSignature,
                outputSignature,
                mode,
                admin,
                deprecated != null && !deprecated.isEmpty(),
                deprecated,
                description,
                warning,
                eager,
                caseInsensitive,
                systemProcedure,
                internal,
                allowExpiredCredentials,
                threadSafe,
                CypherScope.ALL_SCOPES);
    }

    @Deprecated(forRemoval = true)
    public ProcedureSignature(
            QualifiedName name,
            List<FieldSignature> inputSignature,
            List<FieldSignature> outputSignature,
            Mode mode,
            boolean admin,
            boolean isDeprecated,
            String deprecatedBy,
            String description,
            String warning,
            boolean eager,
            boolean caseInsensitive,
            boolean systemProcedure,
            boolean internal,
            boolean allowExpiredCredentials,
            boolean threadSafe) {
        this(
                name,
                inputSignature,
                outputSignature,
                mode,
                admin,
                isDeprecated,
                deprecatedBy,
                description,
                warning,
                eager,
                caseInsensitive,
                systemProcedure,
                internal,
                allowExpiredCredentials,
                threadSafe,
                CypherScope.ALL_SCOPES);
    }

    public ProcedureSignature(
            QualifiedName name,
            List<FieldSignature> inputSignature,
            List<FieldSignature> outputSignature,
            Mode mode,
            boolean admin,
            boolean isDeprecated,
            String deprecatedBy,
            String description,
            String warning,
            boolean eager,
            boolean caseInsensitive,
            boolean systemProcedure,
            boolean internal,
            boolean allowExpiredCredentials,
            boolean threadSafe,
            Set<CypherScope> supportedCypherScopes) {
        this(
                name,
                inputSignature,
                outputSignature,
                mode,
                admin,
                isDeprecated,
                deprecatedBy,
                description,
                warning,
                eager,
                caseInsensitive,
                systemProcedure,
                internal,
                allowExpiredCredentials,
                threadSafe,
                supportedCypherScopes,
                new UnsupportedDatabaseTypes.DatabaseType[0]);
    }

    public ProcedureSignature(
            QualifiedName name,
            List<FieldSignature> inputSignature,
            List<FieldSignature> outputSignature,
            Mode mode,
            boolean admin,
            boolean isDeprecated,
            String deprecatedBy,
            String description,
            String warning,
            boolean eager,
            boolean caseInsensitive,
            boolean systemProcedure,
            boolean internal,
            boolean allowExpiredCredentials,
            boolean threadSafe,
            Set<CypherScope> supportedCypherScopes,
            UnsupportedDatabaseTypes.DatabaseType[] unsupportedDbTypes) {
        this.name = name;
        this.inputSignature = unmodifiableList(inputSignature);
        this.outputSignature = outputSignature == VOID ? outputSignature : unmodifiableList(outputSignature);
        this.mode = mode;
        this.admin = admin;
        this.unsupportedDbTypes = unsupportedDbTypes;
        this.isDeprecated = isDeprecated || (deprecatedBy != null && !deprecatedBy.isEmpty());
        this.deprecated = deprecatedBy;
        this.description = description;
        this.warning = warning;
        this.eager = eager;
        this.caseInsensitive = caseInsensitive;
        this.systemProcedure = systemProcedure;
        this.internal = internal;
        this.allowExpiredCredentials = allowExpiredCredentials;
        this.threadSafe = threadSafe;
        this.supportedCypherScopes = supportedCypherScopes;
    }

    public QualifiedName name() {
        return name;
    }

    public Mode mode() {
        return mode;
    }

    public boolean admin() {
        return admin;
    }

    public boolean isDeprecated() {
        return isDeprecated;
    }

    public Optional<String> deprecated() {
        return Optional.ofNullable(deprecated);
    }

    public boolean caseInsensitive() {
        return caseInsensitive;
    }

    public List<FieldSignature> inputSignature() {
        return inputSignature;
    }

    public List<FieldSignature> outputSignature() {
        return outputSignature;
    }

    public boolean isVoid() {
        return outputSignature == VOID;
    }

    public Optional<String> description() {
        return Optional.ofNullable(description);
    }

    public Optional<String> warning() {
        return Optional.ofNullable(warning);
    }

    public boolean eager() {
        return eager;
    }

    public boolean systemProcedure() {
        return systemProcedure;
    }

    public boolean internal() {
        return internal;
    }

    public UnsupportedDatabaseTypes.DatabaseType[] unsupportedDbTypes() {
        return unsupportedDbTypes;
    }

    public boolean allowedExpiredCredentials() {
        return allowExpiredCredentials;
    }

    public boolean threadSafe() {
        return threadSafe;
    }

    public Set<CypherScope> supportedCypherScopes() {
        return this.supportedCypherScopes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ProcedureSignature that = (ProcedureSignature) o;
        return name.equals(that.name)
                && inputSignature.equals(that.inputSignature)
                && outputSignature.equals(that.outputSignature)
                && isVoid() == that.isVoid();
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public String toString() {
        String strInSig = inputSignature == null ? "..." : Iterables.toString(inputSignature, ", ");
        if (isVoid()) {
            return String.format("%s(%s)", name, strInSig);
        } else {
            String strOutSig = outputSignature == null ? "..." : Iterables.toString(outputSignature, ", ");
            return String.format("%s(%s) :: (%s)", name, strInSig, strOutSig);
        }
    }

    public static class Builder {
        private final QualifiedName name;
        private final List<FieldSignature> inputSignature = new ArrayList<>();
        private List<FieldSignature> outputSignature = new ArrayList<>();
        private Mode mode = Mode.READ;
        private Boolean isDeprecated = false;
        private String deprecated;
        private String description;
        private String warning;
        private boolean eager;
        private boolean admin;
        private boolean systemProcedure;
        private boolean internal;
        private boolean allowExpiredCredentials;
        private boolean threadSafe;

        private Set<CypherScope> supportedCypherScopes = CypherScope.ALL_SCOPES;

        public Builder(QualifiedName name) {
            this.name = name;
            this.threadSafe = true;
        }

        public Builder mode(Mode mode) {
            this.mode = mode;
            return this;
        }

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public Builder isDeprecated(Boolean isDeprecated) {
            this.isDeprecated = isDeprecated;
            return this;
        }

        public Builder deprecatedBy(String deprecated) {
            this.isDeprecated = deprecated != null && !deprecated.isEmpty();
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
        public Builder out(String name, Neo4jTypes.AnyType type) {
            outputSignature.add(FieldSignature.outputField(name, type));
            return this;
        }

        public Builder out(List<FieldSignature> fields) {
            outputSignature = fields;
            return this;
        }

        public Builder admin(boolean admin) {
            this.admin = admin;
            return this;
        }

        public Builder warning(String warning) {
            this.warning = warning;
            return this;
        }

        public Builder eager(boolean eager) {
            this.eager = eager;
            return this;
        }

        public Builder systemProcedure() {
            this.systemProcedure = true;
            return this;
        }

        public Builder internal() {
            this.internal = true;
            return this;
        }

        public Builder allowExpiredCredentials() {
            this.allowExpiredCredentials = true;
            return this;
        }

        public Builder threadSafe() {
            this.threadSafe = true;
            return this;
        }

        public Builder notThreadSafe() {
            this.threadSafe = false;
            return this;
        }

        public Builder supportedCypherScopes(CypherScope... versions) {
            this.supportedCypherScopes = EnumSet.copyOf(Arrays.asList(versions));
            return this;
        }

        public ProcedureSignature build() {
            return new ProcedureSignature(
                    name,
                    inputSignature,
                    outputSignature,
                    mode,
                    admin,
                    isDeprecated || (deprecated != null && !deprecated.isEmpty()),
                    deprecated,
                    description,
                    warning,
                    eager,
                    false,
                    systemProcedure,
                    internal,
                    allowExpiredCredentials,
                    threadSafe,
                    supportedCypherScopes);
        }
    }

    public static Builder procedureSignature(QualifiedName name) {
        return new Builder(name);
    }
}
