/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.perftest.enterprise.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static java.util.Collections.addAll;
import static java.util.Collections.emptyList;

public abstract class Setting<T>
{
    public static Setting<String> stringSetting( String name )
    {
        return stringSetting( name, null );
    }

    public static Setting<String> stringSetting( String name, String defaultValue )
    {
        return new Setting<String>( name, defaultValue )
        {
            @Override
            String parse( String value )
            {
                return value;
            }
        };
    }

    public static Setting<Long> integerSetting( String name, long defaultValue )
    {
        return new Setting<Long>( name, defaultValue )
        {
            @Override
            Long parse( String value )
            {
                return Long.parseLong( value );
            }
        };
    }

    public static Setting<Boolean> booleanSetting( String name, boolean defaultValue )
    {
        return new Setting<Boolean>( name, defaultValue )
        {
            @Override
            Boolean parse( String value )
            {
                return Boolean.parseBoolean( value );
            }

            @Override
            boolean isBoolean()
            {
                return true;
            }
        };
    }

    public static <T> Setting<T> restrictSetting( Setting<T> setting, Predicate<? super T> firstPredicate,
                                                  Predicate<? super T>... morePredicates )
    {
        final Collection<Predicate<? super T>> predicates;
        if ( morePredicates == null || morePredicates.length == 0 )
        {
            predicates = new ArrayList<Predicate<? super T>>( 1 );
            predicates.add( firstPredicate );
        }
        else
        {
            predicates = new ArrayList<Predicate<? super T>>( 1 + morePredicates.length );
            predicates.add( firstPredicate );
            addAll( predicates, morePredicates );
        }
        return new SettingAdapter<T, T>( setting )
        {
            @Override
            T adapt( T value )
            {
                for ( Predicate<? super T> predicate : predicates )
                {
                    if ( !predicate.matches( value ) )
                    {
                        throw new IllegalArgumentException(
                                String.format( "'%s' does not match %s", value, predicate ) );
                    }
                }
                return value;
            }
        };
    }

    public static <T extends Enum<T>> Setting<T> enumSetting( final Class<T> enumType, String name )
    {
        return new Setting<T>( name, null )
        {
            @Override
            T parse( String value )
            {
                return Enum.valueOf( enumType, value );
            }
        };
    }

    public static <T extends Enum<T>> Setting<T> enumSetting( String name, T defaultValue )
    {
        final Class<T> enumType = defaultValue.getDeclaringClass();
        return new Setting<T>( name, defaultValue )
        {
            @Override
            T parse( String value )
            {
                return Enum.valueOf( enumType, value );
            }
        };
    }

    public static <T> Setting<List<T>> listSetting( final Setting<T> singleSetting, final List<T> defaultValue )
    {
        return new Setting<List<T>>( singleSetting.name(), defaultValue )
        {
            @Override
            List<T> parse( String value )
            {
                if ( value.trim().equals( "" ) )
                {
                    return emptyList();
                }
                String[] parts = value.split( "," );
                List<T> result = new ArrayList<T>( parts.length );
                for ( String part : parts )
                {
                    result.add( singleSetting.parse( part ) );
                }
                return result;
            }

            @Override
            public String asString( List<T> value )
            {
                StringBuilder result = new StringBuilder();
                Iterator<T> iterator = value.iterator();
                while ( iterator.hasNext() )
                {
                    result.append( singleSetting.asString( iterator.next() ) );
                    if ( iterator.hasNext() )
                    {
                        result.append( ',' );
                    }
                }
                return result.toString();
            }
        };
    }

    public static <FROM, TO> Setting<TO> adaptSetting( Setting<FROM> source,
                                                       final Conversion<? super FROM, TO> conversion )
    {
        return new SettingAdapter<FROM, TO>( source )
        {
            @Override
            TO adapt( FROM value )
            {
                return conversion.convert( value );
            }
        };
    }

    private final String name;
    private final T defaultValue;

    private Setting( String name, T defaultValue )
    {
        this.name = name;
        this.defaultValue = defaultValue;
    }

    @Override
    public String toString()
    {
        return String.format( "Setting[%s]", name );
    }

    public String name()
    {
        return name;
    }

    T defaultValue()
    {
        if ( defaultValue == null )
        {
            throw new IllegalStateException( String.format( "Required setting '%s' not configured.", name ) );
        }
        return defaultValue;
    }

    abstract T parse( String value );

    boolean isBoolean()
    {
        return false;
    }

    void validateValue( String value )
    {
        parse( value );
    }

    public String asString( T value )
    {
        return value.toString();
    }

    private static abstract class SettingAdapter<FROM, TO> extends Setting<TO>
    {
        private final Setting<FROM> source;

        SettingAdapter( Setting<FROM> source )
        {
            super( source.name, null );
            this.source = source;
        }

        @Override
        TO parse( String value )
        {
            return adapt( source.parse( value ) );
        }

        abstract TO adapt( FROM value );

        @Override
        TO defaultValue()
        {
            return adapt( source.defaultValue() );
        }
    }
}
