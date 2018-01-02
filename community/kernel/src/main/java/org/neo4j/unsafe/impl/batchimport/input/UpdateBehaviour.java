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
package org.neo4j.unsafe.impl.batchimport.input;

import java.lang.reflect.Array;
import java.util.Arrays;

/**
 * Decides how to merge two property values for a given key.
 */
public enum UpdateBehaviour
{
    /**
     * Replaces any existing value with a new value.
     */
    OVERWRITE
    {
        @Override
        public Object merge( Object existing, Object additional )
        {
            return additional;
        }
    },
    /**
     * Adds the new value to the existing one by adding one more element in the existing array,
     * or if the existing value is only a single value, an array with length 2 will be created.
     */
    ADD
    {
        @Override
        public Object merge( Object existing, Object additional )
        {
            Object[] result;
            if ( existing.getClass().isArray() )
            {
                result = Arrays.copyOf( (Object[]) existing, Array.getLength( existing )+1 );
                result[result.length-1] = additional;
            }
            else if ( additional.getClass().isArray() )
            {
                int length = Array.getLength( additional );
                result = (Object[]) Array.newInstance( existing.getClass(), 1+length );
                result[0] = existing;
                for ( int i = 0; i < length; i++ )
                {
                    result[1+i] = Array.get( additional, i );
                }
            }
            else
            {
                result = (Object[]) Array.newInstance( existing.getClass(), 2 );
                result[0] = existing;
                result[1] = additional;
            }
            return result;
        }
    };

    public abstract Object merge( Object existing, Object additional );
}
