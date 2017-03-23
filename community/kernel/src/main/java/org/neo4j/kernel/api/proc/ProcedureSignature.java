/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import org.neo4j.procedure.Mode;

import static java.util.Collections.unmodifiableList;

/**
 * This describes the signature of a procedure, made up of its namespace, name, and input/output description.
 * Procedure uniqueness is currently *only* on the namespace/name level - no procedure overloading allowed (yet).
 */
public class ProcedureSignature
{
    public static List<FieldSignature> VOID = unmodifiableList( new ArrayList<>() );

    private final QualifiedName name;
    private final List<FieldSignature> inputSignature;
    private final List<FieldSignature> outputSignature;
    private final Mode mode;
    private final Optional<String> deprecated;
    private final String[] allowed;
    private final Optional<String> description;
    private final Optional<String> warning;

    public ProcedureSignature( QualifiedName name,
            List<FieldSignature> inputSignature,
            List<FieldSignature> outputSignature,
            Mode mode,
            Optional<String> deprecated,
            String[] allowed,
            Optional<String> description,
            Optional<String> warning)
    {
        this.name = name;
        this.inputSignature = unmodifiableList( inputSignature );
        this.outputSignature = outputSignature == VOID ? outputSignature : unmodifiableList( outputSignature );
        this.mode = mode;
        this.deprecated = deprecated;
        this.allowed = allowed;
        this.description = description;
        this.warning = warning;
    }

    public QualifiedName name()
    {
        return name;
    }

    public Mode mode()
    {
        return mode;
    }

    public Optional<String> deprecated()
    {
        return deprecated;
    }

    public String[] allowed()
    {
        return allowed;
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

    public Optional<String> warning()
    {
        return warning;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        ProcedureSignature that = (ProcedureSignature) o;
        return name.equals( that.name ) && inputSignature.equals( that.inputSignature ) &&
                outputSignature.equals( that.outputSignature ) && isVoid() == that.isVoid();
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
        private final QualifiedName name;
        private final List<FieldSignature> inputSignature = new LinkedList<>();
        private List<FieldSignature> outputSignature = new LinkedList<>();
        private Mode mode = Mode.READ;
        private Optional<String> deprecated = Optional.empty();
        private String[] allowed = new String[0];
        private Optional<String> description = Optional.empty();
        private Optional<String> warning = Optional.empty();

        public Builder( String[] namespace, String name )
        {
            this.name = new QualifiedName( namespace, name );
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
            inputSignature.add( FieldSignature.inputField( name, type ) );
            return this;
        }

        /** Define an output field */
        public Builder out( String name, AnyType type )
        {
            outputSignature.add( FieldSignature.outputField( name, type ) );
            return this;
        }

        public Builder out( List<FieldSignature> fields )
        {
            outputSignature = fields;
            return this;
        }

        public Builder allowed( String[] allowed )
        {
            this.allowed = allowed;
            return this;
        }

        public Builder warning( String warning )
        {
            this.warning = Optional.of( warning );
            return this;
        }

        public ProcedureSignature build()
        {
            return new ProcedureSignature(name, inputSignature, outputSignature, mode, deprecated, allowed, description, warning );
        }
    }

    public static Builder procedureSignature(String ... namespaceAndName)
    {
        String[] namespace = namespaceAndName.length > 1 ? Arrays.copyOf( namespaceAndName, namespaceAndName.length - 1 ) : new String[0];
        String name = namespaceAndName[namespaceAndName.length - 1];
        return procedureSignature( namespace, name );
    }

    public static Builder procedureSignature( QualifiedName name )
    {
        return new Builder( name.namespace(), name.name() );
    }

    public static Builder procedureSignature(String[] namespace, String name)
    {
        return new Builder(namespace, name);
    }

    public static QualifiedName procedureName( String ... namespaceAndName)
    {
        return procedureSignature( namespaceAndName ).build().name();
    }
}
