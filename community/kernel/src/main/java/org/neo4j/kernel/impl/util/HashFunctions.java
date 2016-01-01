/**
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
package org.neo4j.kernel.impl.util;

/**
 * Based off of CERNs COLT library, see LICENSES.txt for details.
 */
public class HashFunctions extends Object {
    /**
     * Makes this class non instantiable, but still let's others inherit from
     * it.
     */
    protected HashFunctions() {
    }

    /**
     * Returns a hashcode for the specified value.
     *
     * @return a hash code value for the specified value.
     */
    public static int hash(char value) {
        return value;
    }

    /**
     * Returns a hashcode for the specified value.
     *
     * @return a hash code value for the specified value.
     */
    public static int hash(double value) {
        long bits = Double.doubleToLongBits(value);
        return (int) (bits ^ (bits >>> 32));
    }

    /**
     * Returns a hashcode for the specified value.
     *
     * @return a hash code value for the specified value.
     */
    public static int hash(float value) {
        return Float.floatToIntBits(value * 663608941.737f);
        // this avoids excessive hashCollisions in the case values are of the
        // form (1.0, 2.0, 3.0, ...)
    }

    /**
     * Returns a hashcode for the specified value.
     *
     * @return a hash code value for the specified value.
     */
    public static int hash(int value) {
        return value;
    }

    /**
     * Returns a hashcode for the specified value.
     *
     * @return a hash code value for the specified value.
     */
    public static int hash(long value) {
        return (int) (value ^ (value >> 32));
    }

    /**
     * Returns a hashcode for the specified object.
     *
     * @return a hash code value for the specified object.
     */
    public static int hash(Object object) {
        return object == null ? 0 : object.hashCode();
    }

    /**
     * Returns a hashcode for the specified value.
     *
     * @return a hash code value for the specified value.
     */
    public static int hash(short value) {
        return value;
    }

    /**
     * Returns a hashcode for the specified value.
     *
     * @return a hash code value for the specified value.
     */
    public static int hash(boolean value) {
        return value ? 1231 : 1237;
    }

    /**
     * Returns a hashcode for the specified value.
     *
     * @return a hash code value for the specified value.
     */
    public static int hash(byte value) {
        return value;
    }
}