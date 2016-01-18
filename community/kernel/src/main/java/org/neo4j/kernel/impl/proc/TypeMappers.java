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
package org.neo4j.kernel.impl.proc;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.proc.Neo4jTypes;
import org.neo4j.kernel.api.proc.Neo4jTypes.AnyType;

import static org.neo4j.kernel.api.proc.Neo4jTypes.NTAny;
import static org.neo4j.kernel.api.proc.Neo4jTypes.NTBoolean;
import static org.neo4j.kernel.api.proc.Neo4jTypes.NTFloat;
import static org.neo4j.kernel.api.proc.Neo4jTypes.NTInteger;
import static org.neo4j.kernel.api.proc.Neo4jTypes.NTList;
import static org.neo4j.kernel.api.proc.Neo4jTypes.NTMap;
import static org.neo4j.kernel.api.proc.Neo4jTypes.NTNumber;
import static org.neo4j.kernel.api.proc.Neo4jTypes.NTString;

public class TypeMappers
{
    /**
     * Converts a java object to the specified {@link #type() neo4j type}. In practice, this is
     * often the same java object - but this gives a guarantee that only java objects Neo4j can
     * digest are outputted.
     */
    interface NeoValueConverter
    {
        AnyType type();
        Object toNeoValue( Object javaValue ) throws ProcedureException;
    }

    private final Map<Type,NeoValueConverter> javaToNeo = new HashMap<>();

    public TypeMappers()
    {
        registerScalarsAndCollections();
    }

    /**
     * We don't have Node, Relationship, Property available down here - and don't strictly want to,
     * we want the procedures to be independent of which Graph API is being used (and we don't want
     * them to get tangled up with kernel code). So, we only register the "core" type system here,
     * scalars and collection types. Node, Relationship, Path and any other future graph types should
     * be registered from the outside in the same place APIs to work with those types is registered.
     */
    private void registerScalarsAndCollections()
    {
        registerType( String.class, TO_STRING );
        registerType( long.class, TO_INTEGER );
        registerType( Long.class, TO_INTEGER );
        registerType( double.class, TO_FLOAT );
        registerType( Double.class, TO_FLOAT );
        registerType( Number.class, TO_NUMBER );
        registerType( boolean.class, TO_BOOLEAN );
        registerType( Boolean.class, TO_BOOLEAN );
        registerType( Map.class, TO_MAP );
        registerType( List.class, TO_LIST );
        registerType( Object.class, TO_ANY );
    }

    public AnyType neoTypeFor( Type javaType ) throws ProcedureException
    {
        return converterFor( javaType ).type();
    }

    public NeoValueConverter converterFor( Type javaType ) throws ProcedureException
    {
        NeoValueConverter converter = javaToNeo.get( javaType );
        if( converter != null )
        {
            return converter;
        }

        if( javaType instanceof ParameterizedType )
        {
            ParameterizedType pt = (ParameterizedType) javaType;
            Type rawType = pt.getRawType();

            if( rawType == List.class )
            {
                return toList( converterFor( pt.getActualTypeArguments()[0] ) );
            }
            else if( rawType == Map.class )
            {
                Type type = pt.getActualTypeArguments()[0];
                if( type != String.class )
                {
                    throw new ProcedureException( Status.Procedure.FailedRegistration,
                            "Maps are required to have `String` keys - but this map has `%s` keys.",
                            type.getTypeName() );
                }
                return TO_MAP;
            }
        }

        throw javaToNeoMappingError( javaType, Neo4jTypes.NTAny );
    }

    public void registerType( Class<?> javaClass, NeoValueConverter toNeo )
    {
        javaToNeo.put( javaClass, toNeo );
    }

    private final NeoValueConverter TO_ANY = new SimpleConverter( NTAny, Object.class );
    private final NeoValueConverter TO_STRING = new SimpleConverter( NTString, String.class );
    private final NeoValueConverter TO_INTEGER = new SimpleConverter( NTInteger, Long.class );
    private final NeoValueConverter TO_FLOAT = new SimpleConverter( NTFloat, Double.class );
    private final NeoValueConverter TO_NUMBER = new SimpleConverter( NTNumber, Number.class );
    private final NeoValueConverter TO_BOOLEAN = new SimpleConverter( NTBoolean, Boolean.class );
    private final NeoValueConverter TO_MAP = new SimpleConverter( NTMap, Map.class );
    private final NeoValueConverter TO_LIST = toList( TO_ANY );

    private NeoValueConverter toList( NeoValueConverter inner )
    {
        return new SimpleConverter( NTList( inner.type() ), List.class );
    }

    private static ProcedureException javaToNeoMappingError( Type cls, AnyType neoType )
    {
        return new ProcedureException( Status.Statement.InvalidType, "Don't know how to map `%s` to `%s`", cls, neoType );
    }

    public static class SimpleConverter implements NeoValueConverter
    {
        private final AnyType type;
        private final Class<?> javaClass;

        public SimpleConverter( AnyType type, Class<?> javaClass )
        {
            this.type = type;
            this.javaClass = javaClass;
        }

        @Override
        public AnyType type()
        {
            return type;
        }

        @Override
        public Object toNeoValue( Object javaValue ) throws ProcedureException
        {
            if( javaValue == null || javaClass.isInstance( javaValue ) )
            {
                return javaValue;
            }
            throw new ProcedureException( Status.Procedure.CallFailed,
                    "Expected `%s` to be a `%s`, found `%s`.", javaValue, javaClass.getSimpleName(), javaValue.getClass());
        }
    }
}
