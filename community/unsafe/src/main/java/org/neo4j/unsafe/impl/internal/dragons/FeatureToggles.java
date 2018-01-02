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
package org.neo4j.unsafe.impl.internal.dragons;

/**
 * Feature toggles are used for features that are possible to configure, but where the configuration is always fixed in
 * a production system.
 *
 * Typical use cases for feature toggles are features that are integrated into the code base for integration and
 * testing purposes, but are not ready for inclusion in the finished product yet, or features that are always on or has
 * a fixed configured value in the finished product, but can assume a different configuration or be turned off in some
 * test.
 *
 * Feature toggles are passed to the JVM through {@linkplain System#getProperty(String) system properties}, and
 * expected to be looked up from a static context, a {@code static final} fields of the class the toggle controls.
 *
 * All methods in this class returns the default value if the system property has not been assigned, or if the value of
 * the system property cannot be interpreted as a value of the expected type.
 *
 * For features that the user is ever expected to touch, feature toggles is the wrong abstraction!
 */
public class FeatureToggles
{
    /**
     * Get the value of a {@code boolean} system property.
     *
     * The absolute name of the system property is computed based on the provided class and local name.
     *
     * @param location the class that owns the flag.
     * @param name the local name of the flag.
     * @param defaultValue the default value of the flag if the system property is not assigned.
     * @return the parsed value of the system property, or the default value.
     */
    public static boolean flag( Class<?> location, String name, boolean defaultValue )
    {
        return booleanProperty( name( location, name ), defaultValue );
    }

    /**
     * Get the value of a {@code boolean} system property.
     *
     * The absolute name of the system property is computed based on the package of the provided class and local name.
     *
     * @param location a class in the package that owns the flag.
     * @param name the local name of the flag.
     * @param defaultValue the default value of the flag if the system property is not assigned.
     * @return the parsed value of the system property, or the default value.
     */
    public static boolean packageFlag( Class<?> location, String name, boolean defaultValue )
    {
        return booleanProperty( name( location.getPackage(), name ), defaultValue );
    }

    /**
     * Get the value of a {@code long} system property.
     *
     * The absolute name of the system property is computed based on the provided class and local name.
     *
     * @param location the class that owns the flag.
     * @param name the local name of the flag.
     * @param defaultValue the default value of the flag if the system property is not assigned.
     * @return the parsed value of the system property, or the default value.
     */
    public static long getLong( Class<?> location, String name, long defaultValue )
    {
        return Long.getLong( name( location, name ), defaultValue );
    }

    /**
     * Get the value of a {@code int} system property.
     *
     * The absolute name of the system property is computed based on the provided class and local name.
     *
     * @param location the class that owns the flag.
     * @param name the local name of the flag.
     * @param defaultValue the default value of the flag if the system property is not assigned.
     * @return the parsed value of the system property, or the default value.
     */
    public static int getInteger( Class<?> location, String name, int defaultValue )
    {
        return Integer.getInteger( name( location, name ), defaultValue );
    }

    /**
     * Get the value of a {@code enum} system property.
     *
     * The absolute name of the system property is computed based on the provided class and local name.
     *
     * @param location the class that owns the flag.
     * @param name the local name of the flag.
     * @param defaultValue the default value of the flag if the system property is not assigned.
     * @param <E> the enum value type.
     * @return the parsed value of the system property, or the default value.
     */
    public static <E extends Enum<E>> E flag( Class<?> location, String name, E defaultValue )
    {
        return enumProperty( defaultValue.getDeclaringClass(), name( location, name ), defaultValue );
    }

    /**
     * Helps creating a JVM parameter for setting a {@code boolean} feature toggle.
     *
     * @param location the class that owns the flag.
     * @param name the local name of the flag.
     * @param value the value to assign to the feature toggle.
     * @return the parameter to pass to the command line of the forked JVM.
     */
    public static String toggle( Class<?> location, String name, boolean value )
    {
        return toggle( name( location, name ), Boolean.toString( value ) );
    }

    /**
     * Helps creating a JVM parameter for setting a {@code long} or {@code int} feature toggle.
     *
     * @param location the class that owns the flag.
     * @param name the local name of the flag.
     * @param value the value to assign to the feature toggle.
     * @return the parameter to pass to the command line of the forked JVM.
     */
    public static String toggle( Class<?> location, String name, long value )
    {
        return toggle( name( location, name ), Long.toString( value ) );
    }

    /**
     * Helps creating a JVM parameter for setting an {@code enum} feature toggle.
     *
     * @param location the class that owns the flag.
     * @param name the local name of the flag.
     * @param value the value to assign to the feature toggle.
     * @return the parameter to pass to the command line of the forked JVM.
     */
    public static String toggle( Class<?> location, String name, Enum<?> value )
    {
        return toggle( name( location, name ), value.name() );
    }

    // <implementation>

    private static String toggle( String key, String value )
    {
        return "-D" + key + "=" + value;
    }

    private FeatureToggles()
    {
    }

    private static String name( Class<?> location, String name )
    {
        return location.getCanonicalName() + "." + name;
    }

    private static String name( Package location, String name )
    {
        return location.getName() + "." + name;
    }

    private static boolean booleanProperty( String flag, boolean defaultValue )
    {
        return parseBoolean( System.getProperty( flag ), defaultValue );
    }

    private static boolean parseBoolean( String value, boolean defaultValue )
    {
        return defaultValue ? !"false".equalsIgnoreCase( value ) : "true".equalsIgnoreCase( value );
    }

    private static <E extends Enum<E>> E enumProperty( Class<E> enumClass, String name, E defaultValue )
    {
        try
        {
            return Enum.valueOf( enumClass, System.getProperty( name, defaultValue.name() ) );
        }
        catch ( IllegalArgumentException e )
        {
            return defaultValue;
        }
    }
}
