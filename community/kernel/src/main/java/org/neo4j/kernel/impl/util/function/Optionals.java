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
package org.neo4j.kernel.impl.util.function;

import org.neo4j.function.Function;

public class Optionals
{
    private static final Optional NONE = new Optional()
    {
        @Override
        public Object get()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isPresent()
        {
            return false;
        }

        @Override
        public Optional or( Optional secondChoice )
        {
            return secondChoice;
        }

        @Override
        public Optional or( Object secondChoice )
        {
            return some( secondChoice );
        }

        @Override
        public Optional map( Function conversion )
        {
            return this;
        }

        @Override
        public String toString()
        {
            return "none";
        }
    };

    @SuppressWarnings("unchecked")
    public static <TYPE> Optional<TYPE> none()
    {
        return NONE;
    }

    public static <TYPE> Optional<TYPE> some( final TYPE obj )
    {
        return new Optional<TYPE>()
        {
            @Override
            public TYPE get()
            {
                return obj;
            }

            @Override
            public boolean isPresent()
            {
                return true;
            }

            @Override
            public Optional<TYPE> or( Optional<TYPE> secondChoice )
            {
                return this;
            }

            @Override
            public Optional<TYPE> or( TYPE secondChoice )
            {
                return this;
            }

            @SuppressWarnings("RedundantTypeArguments")
            @Override
            public <To> Optional<To> map( Function<TYPE, ? extends To> conversion )
            {
                return Optionals.<To>some( conversion.apply( obj ) );
            }

            @Override
            public String toString()
            {
                return "some( " + obj.toString() + " )";
            }
        };
    }
}
