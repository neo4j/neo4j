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
package org.neo4j.kernel.impl.index.schema.fusion;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Utility methods for working with multiple sub-components within the Fusion Index system
 */
public abstract class FusionIndexUtils
{
    interface ActionAble
    {
        void doIt( Object obj ) throws Exception;
    }

    /**
     * Method for calling a lambda function on many objects when it is expected that the function might
     * throw an exception. The method will catch all exceptions from all calls, and throw the first exception
     * at the end. This is equivalent to have a set of nested try{}finally{} blocks to ensure all calls are made,
     * but is generalized to any number of calls.
     *
     * For example, in FusionIndexAccessor:
     * <pre>
     *    public void drop() throws IOException
     *    {
     *        forAll( ( accessor ) -> ((IndexAccessor) accessor).drop(), nativeAccessor, spatialAccessor, luceneAccessor );
     *        dropAction.drop( indexId );
     *    }
     * </pre>
     *
     * @param actionable lambda function to call on each object passed
     * @param accessors varargs array of objects to call the function on
     * @param <E> the type of exception anticipated, inferred from the lambda
     * @throws E
     */
    public static <E extends Exception> void forAll( ActionAble actionable, Object... accessors ) throws E
    {
        List<E> error = Arrays.stream( accessors ).map( accessor ->
        {
            try
            {
                actionable.doIt( accessor );
                return null;
            }
            catch ( Throwable t )
            {
                return (E) t;
            }
        } ).filter( Objects::nonNull ).collect( Collectors.toList() );
        if ( !error.isEmpty() )
        {
            throw error.get(0);
        }
    }
}
