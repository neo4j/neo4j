/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.proc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.proc.Neo4jTypes.AnyType;
import org.neo4j.kernel.impl.proc.Neo4jValue;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

/**
 * This describes the signature of a procedure, made up of its namespace, name, and input/output description.
 * Procedure uniqueness is currently *only* on the namespace/name level - no procedure overloading allowed (yet).
 */
public class ProcedureSignature
{
    public static List<FieldSignature> VOID = unmodifiableList( new ArrayList<>() );

    public static class ProcedureName
    {
        private final String[] namespace;
        private final String name;

        public ProcedureName( List<String> namespace, String name )
        {
            this(namespace.toArray( new String[namespace.size()] ), name);
        }

        public ProcedureName( String[] namespace, String name )
        {
            this.namespace = namespace;
            this.name = name;
        }

        public String[] namespace()
        {
            return namespace;
        }

        public String name()
        {
            return name;
        }

        @Override
        public String toString()
        {
            String strNamespace = namespace.length > 0 ? Iterables.toString( asList( namespace ), "." ) + "." : "";
            return String.format("%s%s", strNamespace, name);
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o ) { return true; }
            if ( o == null || getClass() != o.getClass() ) { return false; }

            ProcedureName that = (ProcedureName) o;
            return Arrays.equals( namespace, that.namespace ) && name.equals( that.name );
        }

        @Override
        public int hashCode()
        {
            int result = Arrays.hashCode( namespace );
            result = 31 * result + name.hashCode();
            return result;
        }
    }

    /** Represents a type and a name for a field in a record, used to define input and output record signatures. */
    public static class FieldSignature
    {
        private final String name;
        private final AnyType type;
        private final Optional<Neo4jValue> defaultValue;

        public FieldSignature( String name, AnyType type)
        {
            this(name, type, Optional.empty());
        }

        public FieldSignature( String name, AnyType type, Optional<Neo4jValue> defaultValue )
        {
            this.name = name;
            this.type = type;
            this.defaultValue = defaultValue;
        }

        public String name()
        {
            return name;
        }

        public AnyType neo4jType()
        {
            return type;
        }

        public Optional<Neo4jValue> defaultValue()
        {
            return defaultValue;
        }

        @Override
        public String toString()
        {
            String nameValue = defaultValue.isPresent() ? name + " = " + defaultValue.get().value() : name;
            return String.format("%s :: %s", nameValue, type);
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o ) { return true; }
            if ( o == null || getClass() != o.getClass() ) { return false; }
            FieldSignature that = (FieldSignature) o;
            return name.equals( that.name ) && type.equals( that.type );
        }

        @Override
        public int hashCode()
        {
            int result = name.hashCode();
            result = 31 * result + type.hashCode();
            return result;
        }
    }

    private final ProcedureName name;
    private final List<FieldSignature> inputSignature;
    private final List<FieldSignature> outputSignature;
    private final Mode mode;
    private final Optional<String> deprecated;
    private final Optional<String> description;

    /**
     * The procedure mode affects how the procedure will execute, and which capabilities
     * it requires.
     */
    public enum Mode
    {
        /** This procedure will only perform read operations against the graph */
        READ_ONLY,
        /** This procedure may perform both read and write operations against the graph */
        READ_WRITE,
        /** This procedure will perform operations against the schema */
        SCHEMA_WRITE,
        /** This procedure will perform system operations - i.e. not against the graph */
        DBMS
    }

    public ProcedureSignature( ProcedureName name,
            List<FieldSignature> inputSignature,
            List<FieldSignature> outputSignature,
            Mode mode,
            Optional<String> deprecated,
            Optional<String> description )
    {
        this.name = name;
        this.inputSignature = unmodifiableList( inputSignature );
        this.outputSignature = outputSignature == VOID ? outputSignature : unmodifiableList( outputSignature );
        this.mode = mode;
        this.deprecated = deprecated;
        this.description = description;
    }

    public ProcedureName name()
    {
        return name;
    }

    public Mode mode() { return mode; }

    public Optional<String> deprecated()
    {
        return deprecated;
    }

    public List<FieldSignature> inputSignature()
    {
        return inputSignature;
    }

    public List<FieldSignature> outputSignature()
    {
        return outputSignature;
    }

    public boolean isVoid()
    {
        return outputSignature == VOID;
    }

    public Optional<String> description()
    {
        return description;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o ) { return true; }
        if ( o == null || getClass() != o.getClass() ) { return false; }

        ProcedureSignature that = (ProcedureSignature) o;

        return
           name.equals( that.name ) &&
           inputSignature.equals( that.inputSignature ) &&
           outputSignature.equals( that.outputSignature ) &&
           isVoid() == that.isVoid();
    }

    @Override
    public int hashCode()
    {
        return name.hashCode();
    }

    @Override
    public String toString()
    {
        String strInSig = inputSignature == null ? "..." : Iterables.toString( inputSignature, ", " );
        if ( isVoid() )
        {
            return String.format( "%s(%s) :: VOID", name, strInSig );
        }
        else
        {
            String strOutSig = outputSignature == null ? "..." : Iterables.toString( outputSignature, ", " );
            return String.format( "%s(%s) :: (%s)", name, strInSig, strOutSig );
        }
    }

    public static class Builder
    {
        private final ProcedureName name;
        private final List<FieldSignature> inputSignature = new LinkedList<>();
        private List<FieldSignature> outputSignature = new LinkedList<>();
        private Mode mode = Mode.READ_ONLY;
        private Optional<String> deprecated = Optional.empty();
        private Optional<String> description = Optional.empty();

        public Builder( String[] namespace, String name )
        {
            this.name = new ProcedureName( namespace, name );
        }

        public Builder mode( Mode mode )
        {
            this.mode = mode;
            return this;
        }

        public Builder description(String description)
        {
            this.description = Optional.of( description );
            return this;
        }

        public Builder deprecatedBy(String deprecated)
        {
            this.deprecated = Optional.of( deprecated );
            return this;
        }

        /** Define an input field */
        public Builder in( String name, AnyType type )
        {
            inputSignature.add( new FieldSignature( name, type) );
            return this;
        }

        /** Define an output field */
        public Builder out( String name, AnyType type )
        {
            outputSignature.add( new FieldSignature( name, type ) );
            return this;
        }

        public Builder out( List<FieldSignature> fields )
        {
            outputSignature = fields;
            return this;
        }

        public ProcedureSignature build()
        {
            return new ProcedureSignature(name, inputSignature, outputSignature, mode, deprecated, description );
        }
    }

    public static Builder procedureSignature(String ... namespaceAndName)
    {
        String[] namespace = namespaceAndName.length > 1 ? Arrays.copyOf( namespaceAndName, namespaceAndName.length - 1 ) : new String[0];
        String name = namespaceAndName[namespaceAndName.length - 1];
        return procedureSignature( namespace, name );
    }

    public static Builder procedureSignature( ProcedureName name )
    {
        return new Builder( name.namespace, name.name );
    }

    public static Builder procedureSignature(String[] namespace, String name)
    {
        return new Builder(namespace, name);
    }

    public static ProcedureName procedureName( String ... namespaceAndName)
    {
        return procedureSignature( namespaceAndName ).build().name();
    }
}
