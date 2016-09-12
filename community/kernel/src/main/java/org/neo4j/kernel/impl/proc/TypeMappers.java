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
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.proc.Neo4jTypes;
import org.neo4j.kernel.api.proc.Neo4jTypes.AnyType;
import org.neo4j.procedure.Name;

import static java.lang.Boolean.parseBoolean;
import static java.lang.Double.parseDouble;
import static java.lang.Long.parseLong;
import static org.neo4j.kernel.api.proc.Neo4jTypes.NTAny;
import static org.neo4j.kernel.api.proc.Neo4jTypes.NTBoolean;
import static org.neo4j.kernel.api.proc.Neo4jTypes.NTFloat;
import static org.neo4j.kernel.api.proc.Neo4jTypes.NTInteger;
import static org.neo4j.kernel.api.proc.Neo4jTypes.NTList;
import static org.neo4j.kernel.api.proc.Neo4jTypes.NTMap;
import static org.neo4j.kernel.api.proc.Neo4jTypes.NTNumber;
import static org.neo4j.kernel.api.proc.Neo4jTypes.NTString;
import static org.neo4j.kernel.impl.proc.Neo4jValue.ntBoolean;
import static org.neo4j.kernel.impl.proc.Neo4jValue.ntFloat;
import static org.neo4j.kernel.impl.proc.Neo4jValue.ntInteger;

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
        Optional<Neo4jValue> defaultValue(Name parameter) throws ProcedureException;
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
                Type type = pt.getActualTypeArguments()[0];
                return toList( converterFor( type ), type );
            }
            else if( rawType == Map.class )
            {
                Type type = pt.getActualTypeArguments()[0];
                if( type != String.class )
                {
                    throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed,
                            "Maps are required to have `String` keys - but this map has `%s` keys.",
                            type.getTypeName() );
                }
                return TO_MAP;
            }
        }

        throw javaToNeoMappingError( javaType );
    }

    public void registerType( Class<?> javaClass, NeoValueConverter toNeo )
    {
        javaToNeo.put( javaClass, toNeo );
    }

    private final NeoValueConverter TO_ANY = new SimpleConverter( NTAny, Object.class );
    private final NeoValueConverter TO_STRING = new SimpleConverter( NTString, String.class, Neo4jValue::ntString );
    private final NeoValueConverter TO_INTEGER = new SimpleConverter( NTInteger, Long.class, s -> ntInteger( parseLong(s) ) );
    private final NeoValueConverter TO_FLOAT = new SimpleConverter( NTFloat, Double.class, s -> ntFloat( parseDouble(s) ));
    private final NeoValueConverter TO_NUMBER = new SimpleConverter( NTNumber, Number.class, s -> {
        try
        {
            return ntInteger( parseLong(s) );
        }
        catch ( NumberFormatException e )
        {
            return ntFloat( parseDouble( s ) );
        }
    });
    private final NeoValueConverter TO_BOOLEAN = new SimpleConverter( NTBoolean, Boolean.class, s -> ntBoolean( parseBoolean(s) ));
    private final NeoValueConverter TO_MAP = new SimpleConverter( NTMap, Map.class, new MapConverter());
    private final NeoValueConverter TO_LIST = toList( TO_ANY, Object.class );

    private NeoValueConverter toList( NeoValueConverter inner, Type type )
    {
        return new SimpleConverter( NTList( inner.type() ), List.class, new ListConverter(type, inner.type() ));
    }

    private ProcedureException javaToNeoMappingError( Type cls )
    {
        List<String> types = Iterables.asList( javaToNeo.keySet() )
                .stream()
                .map( Type::getTypeName )
                .collect( Collectors.toList());
        types.sort( String::compareTo );

        return new ProcedureException( Status.Statement.TypeError,
                "Don't know how to map `%s` to the Neo4j Type System.%n" +
                "Please refer to to the documentation for full details.%n" +
                "For your reference, known types are: %s", cls.getTypeName(), types );
    }

    public static class SimpleConverter implements NeoValueConverter
    {
        private final AnyType type;
        private final Class<?> javaClass;
        private final Function<String,Neo4jValue> defaultConverter;

        public SimpleConverter( AnyType type, Class<?> javaClass )
        {
            this( type, javaClass, nullParser(javaClass, type) );
        }

        public SimpleConverter( AnyType type, Class<?> javaClass, Function<String,Neo4jValue> defaultConverter )
        {
            this.type = type;
            this.javaClass = javaClass;
            this.defaultConverter = defaultConverter;
        }

        public Optional<Neo4jValue> defaultValue( Name parameter ) throws ProcedureException
        {
            String defaultValue = parameter.defaultValue();
            if ( defaultValue.equals( Name.DEFAULT_VALUE ) )
            {
                return Optional.empty();
            }
            else
            {
                try
                {
                    return Optional.of( defaultConverter.apply( defaultValue ) );
                }
                catch ( Exception e )
                {
                    throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed,
                            "Default value `%s` could not be parsed as a %s", parameter.defaultValue(),
                            javaClass.getSimpleName() );
                }
            }
        }

        @Override
        public AnyType type()
        {
            return type;
        }

        @Override
        public Object toNeoValue( Object javaValue ) throws ProcedureException
        {
            if ( javaValue == null || javaClass.isInstance( javaValue ) )
            {
                return javaValue;
            }
            throw new ProcedureException( Status.Procedure.ProcedureCallFailed,
                    "Expected `%s` to be a `%s`, found `%s`.", javaValue, javaClass.getSimpleName(),
                    javaValue.getClass() );
        }

        private static Function<String,Neo4jValue> nullParser( Class<?> javaType, Neo4jTypes.AnyType neoType )
        {
            return s -> {
                if ( s.equalsIgnoreCase( "null" ) )
                {
                    return new Neo4jValue( null, neoType );
                }
                else
                {
                    throw new IllegalArgumentException( String.format( "A %s can only have a `defaultValue = \"null\"",
                            javaType.getSimpleName() ) );
                }

            };
        }
    }
}
