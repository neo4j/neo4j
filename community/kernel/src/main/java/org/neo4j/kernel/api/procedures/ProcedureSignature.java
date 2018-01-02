/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.api.procedures;


import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.Neo4jTypes.AnyType;

import static java.util.Arrays.asList;

/**
 * This describes the signature of a procedure, made up of its namespace, name, and input/output description.
 * Procedure uniqueness is currently *only* on the namespace/name level - no procedure overloading allowed (yet).
 */
public class ProcedureSignature
{
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

            // Probably incorrect - comparing Object[] arrays with Arrays.equals
            if ( !Arrays.equals( namespace, that.namespace ) ) { return false; }
            return name.equals( that.name );
        }

        @Override
        public int hashCode()
        {
            int result = Arrays.hashCode( namespace );
            result = 31 * result + name.hashCode();
            return result;
        }
    }

    /** Represents a type and a name for an input or output argument in a procedure. */
    public static class ArgumentSignature
    {
        private final String name;
        private final AnyType type;

        public ArgumentSignature( String name, AnyType type )
        {
            this.name = name;
            this.type = type;
        }

        public String name()
        {
            return name;
        }

        public AnyType neo4jType()
        {
            return type;
        }
    }

    private final ProcedureName name;
    private final List<ArgumentSignature> inputSignature;
    private final List<ArgumentSignature> outputSignature;

    public ProcedureSignature( ProcedureName name, List<ArgumentSignature> inputSignature, List<ArgumentSignature> outputSignature )
    {
        this.name = name;
        this.inputSignature = inputSignature;
        this.outputSignature = outputSignature;
    }

    public ProcedureSignature( ProcedureName name )
    {
        this( name, null, null );
    }

    public ProcedureName name()
    {
        return name;
    }

    public List<ArgumentSignature> inputSignature()
    {
        return inputSignature;
    }

    public List<ArgumentSignature> outputSignature()
    {
        return outputSignature;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o ) { return true; }
        if ( o == null || getClass() != o.getClass() ) { return false; }

        ProcedureSignature that = (ProcedureSignature) o;

        if ( !name.equals( that.name ) ) { return false; }
        if ( inputSignature != null ? !inputSignature.equals( that.inputSignature ) : that.inputSignature != null ) { return false; }
        return !(outputSignature != null ? !outputSignature.equals( that.outputSignature ) : that.outputSignature != null);

    }

    @Override
    public int hashCode()
    {
        return name.hashCode();
    }

    @Override
    public String toString()
    {
        String strInSig = inputSignature == null ? "..." : Iterables.toString( typesOf( inputSignature ), ", " );
        String strOutSig = outputSignature == null ? "..." : Iterables.toString( typesOf( outputSignature ), ", " );
        return String.format( "%s(%s) : (%s)", name, strInSig, strOutSig );
    }

    public static class Builder
    {
        private final ProcedureName name;
        private final List<ArgumentSignature> inputSignature = new LinkedList<>();
        private final List<ArgumentSignature> outputSignature = new LinkedList<>();

        public Builder( String[] namespace, String name )
        {
            this.name = new ProcedureName( namespace, name );
        }

        /** Define an input field */
        public Builder in( String name, AnyType type )
        {
            inputSignature.add( new ArgumentSignature( name, type ) );
            return this;
        }

        /** Define an output field */
        public Builder out( String name, AnyType type )
        {
            outputSignature.add( new ArgumentSignature( name, type ) );
            return this;
        }

        public ProcedureSignature build()
        {
            return new ProcedureSignature(name, inputSignature, outputSignature );
        }
    }

    public static Builder procedureSignature(String ... namespaceAndName)
    {
        String[] namespace = namespaceAndName.length > 1 ? Arrays.copyOf( namespaceAndName, namespaceAndName.length - 1 ) : new String[0];
        String name = namespaceAndName[namespaceAndName.length - 1];
        return procedureSignature( namespace, name );
    }

    public static Builder procedureSignature(String[] namespace, String name)
    {
        return new Builder(namespace, name);
    }

    private static List<AnyType> typesOf( List<ArgumentSignature> namedSig )
    {
        List<AnyType> out = new LinkedList<>();
        for ( int i = 0; i < namedSig.size(); i++ )
        {
            out.add( namedSig.get( i ).neo4jType() );
        }
        return out;
    }
}
