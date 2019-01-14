/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAmount;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.DefaultParameterValue;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes.AnyType;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.core.EmbeddedProxySPI;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.procedure.Name;
import org.neo4j.values.AnyValue;

import static java.lang.Boolean.parseBoolean;
import static java.lang.Double.parseDouble;
import static java.lang.Long.parseLong;
import static org.neo4j.internal.kernel.api.procs.DefaultParameterValue.ntBoolean;
import static org.neo4j.internal.kernel.api.procs.DefaultParameterValue.ntFloat;
import static org.neo4j.internal.kernel.api.procs.DefaultParameterValue.ntInteger;
import static org.neo4j.internal.kernel.api.procs.DefaultParameterValue.nullValue;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTAny;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTBoolean;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTByteArray;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTDate;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTDateTime;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTDuration;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTFloat;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTInteger;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTList;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTLocalDateTime;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTLocalTime;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTMap;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTNumber;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTString;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTTime;

public class TypeMappers extends DefaultValueMapper
{
    public abstract static class TypeChecker
    {
        final AnyType type;
        final Class<?> javaClass;

        private TypeChecker( AnyType type, Class<?> javaClass )
        {
            this.type = type;
            this.javaClass = javaClass;
        }

        public AnyType type()
        {
            return type;
        }

        public Object typeCheck( Object javaValue ) throws ProcedureException
        {
            if ( javaValue == null || javaClass.isInstance( javaValue ) )
            {
                return javaValue;
            }
            throw new ProcedureException( Status.Procedure.ProcedureCallFailed,
                    "Expected `%s` to be a `%s`, found `%s`.", javaValue, javaClass.getSimpleName(),
                    javaValue.getClass() );
        }

        public AnyValue toValue( Object obj )
        {
            return ValueUtils.of( obj );
        }
    }

    private final Map<Type,DefaultValueConverter> javaToNeo = new HashMap<>();

    /**
     * Used by testing.
     */
    public TypeMappers()
    {
        this( null );
    }

    public TypeMappers( EmbeddedProxySPI proxySPI )
    {
        super( proxySPI );
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
        registerType( byte[].class, TO_BYTEARRAY );
        registerType( ZonedDateTime.class, new DefaultValueConverter( NTDateTime, ZonedDateTime.class ) );
        registerType( LocalDateTime.class, new DefaultValueConverter( NTLocalDateTime, LocalDateTime.class ) );
        registerType( LocalDate.class, new DefaultValueConverter( NTDate, LocalDate.class ) );
        registerType( OffsetTime.class, new DefaultValueConverter( NTTime, OffsetTime.class ) );
        registerType( LocalTime.class, new DefaultValueConverter( NTLocalTime, LocalTime.class ) );
        registerType( TemporalAmount.class, new DefaultValueConverter( NTDuration, TemporalAmount.class ) );
    }

    public AnyType toNeo4jType( Type type ) throws ProcedureException
    {
        return converterFor( type ).type;
    }

    public TypeChecker checkerFor( Type javaType ) throws ProcedureException
    {
        return converterFor( javaType );
    }

    DefaultValueConverter converterFor( Type javaType ) throws ProcedureException
    {
        DefaultValueConverter converter = javaToNeo.get( javaType );
        if ( converter != null )
        {
            return converter;
        }

        if ( javaType instanceof ParameterizedType )
        {
            ParameterizedType pt = (ParameterizedType) javaType;
            Type rawType = pt.getRawType();

            if ( rawType == List.class )
            {
                Type type = pt.getActualTypeArguments()[0];
                return toList( converterFor( type ), type );
            }
            else if ( rawType == Map.class )
            {
                Type type = pt.getActualTypeArguments()[0];
                if ( type != String.class )
                {
                    throw new ProcedureException(
                            Status.Procedure.ProcedureRegistrationFailed,
                            "Maps are required to have `String` keys - but this map has `%s` keys.",
                            type.getTypeName() );
                }
                return TO_MAP;
            }
        }
        throw javaToNeoMappingError( javaType );
    }

    void registerType( Class<?> javaClass, DefaultValueConverter toNeo )
    {
        javaToNeo.put( javaClass, toNeo );
    }

    private static final DefaultValueConverter TO_ANY = new DefaultValueConverter( NTAny, Object.class );
    private static final DefaultValueConverter TO_STRING = new DefaultValueConverter( NTString, String.class,
            DefaultParameterValue::ntString );
    private static final DefaultValueConverter TO_INTEGER = new DefaultValueConverter( NTInteger, Long.class, s ->
            ntInteger( parseLong( s ) ) );
    private static final DefaultValueConverter TO_FLOAT = new DefaultValueConverter( NTFloat, Double.class, s ->
            ntFloat( parseDouble( s ) ) );
    private static final DefaultValueConverter TO_NUMBER = new DefaultValueConverter( NTNumber, Number.class, s ->
    {
        try
        {
            return ntInteger( parseLong( s ) );
        }
        catch ( NumberFormatException e )
        {
            return ntFloat( parseDouble( s ) );
        }
    } );
    private static final DefaultValueConverter TO_BOOLEAN = new DefaultValueConverter( NTBoolean, Boolean.class, s ->
            ntBoolean( parseBoolean( s ) ) );
    private static final DefaultValueConverter TO_MAP =
            new DefaultValueConverter( NTMap, Map.class, new MapConverter() );
    private static final DefaultValueConverter TO_LIST = toList( TO_ANY, Object.class );
    private final DefaultValueConverter TO_BYTEARRAY = new DefaultValueConverter( NTByteArray, byte[].class, new ByteArrayConverter() );

    private static DefaultValueConverter toList( DefaultValueConverter inner, Type type )
    {
        return new DefaultValueConverter( NTList( inner.type() ), List.class, new ListConverter( type, inner.type() ) );
    }

    private ProcedureException javaToNeoMappingError( Type cls )
    {
        List<String> types = Iterables.asList( javaToNeo.keySet() )
                .stream()
                .map( Type::getTypeName )
                .sorted( String::compareTo )
                .collect( Collectors.toList() );

        return new ProcedureException( Status.Statement.TypeError,
                "Don't know how to map `%s` to the Neo4j Type System.%n" +
                        "Please refer to to the documentation for full details.%n" +
                        "For your reference, known types are: %s", cls.getTypeName(), types );
    }

    public static final class DefaultValueConverter extends TypeChecker
    {
        private final Function<String,DefaultParameterValue> parser;

        public DefaultValueConverter( AnyType type, Class<?> javaClass )
        {
            this( type, javaClass, nullParser( javaClass, type ) );
        }

        private DefaultValueConverter( AnyType type, Class<?> javaClass, Function<String,DefaultParameterValue> parser )
        {
            super( type, javaClass );
            this.parser = parser;
        }

        public Optional<DefaultParameterValue> defaultValue( Name parameter ) throws ProcedureException
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
                    return Optional.of( parser.apply( defaultValue ) );
                }
                catch ( Exception e )
                {
                    throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed,
                            "Default value `%s` could not be parsed as a %s", parameter.defaultValue(),
                            javaClass.getSimpleName() );
                }
            }
        }

        private static Function<String,DefaultParameterValue> nullParser( Class<?> javaType, Neo4jTypes.AnyType neoType )
        {
            return s ->
            {
                if ( s.equalsIgnoreCase( "null" ) )
                {
                    return nullValue( neoType );
                }
                else
                {
                    throw new IllegalArgumentException( String.format(
                            "A %s can only have a `defaultValue = \"null\"",
                            javaType.getSimpleName() ) );
                }
            };
        }
    }
}
