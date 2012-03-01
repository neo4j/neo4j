/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

package org.neo4j.helpers;

import org.neo4j.helpers.collection.Iterables;

/**
 * Common specifications
 */
public class Specifications
{
    public static <T> Specification<T> TRUE()
    {
        return new Specification<T>()
        {
            public boolean satisfiedBy( T instance )
            {
                return true;
            }
        };
    }

    public static <T> Specification<T> not( final Specification<T> specification )
    {
        return new Specification<T>()
        {
            public boolean satisfiedBy( T instance )
            {
                return !specification.satisfiedBy( instance );
            }
        };
    }

    public static <T> AndSpecification<T> and( final Specification<T>... specifications )
    {
        return and( Iterables.iterable( specifications ));
    }

    public static <T> AndSpecification<T> and( final Iterable<Specification<T>> specifications )
    {
        return new AndSpecification<T>( specifications );
    }

    public static <T> OrSpecification<T> or( final Specification<T>... specifications )
    {
        return or( Iterables.iterable( specifications ) );
    }

    public static <T> OrSpecification<T> or( final Iterable<Specification<T>> specifications )
    {
        return new OrSpecification<T>( specifications );
    }

    public static <T> Specification<T> in( final T... allowed )
    {
        return in( Iterables.iterable( allowed ) );
    }

    public static <T> Specification<T> in( final Iterable<T> allowed )
    {
        return new Specification<T>()
        {
            public boolean satisfiedBy( T item )
            {
                for( T allow : allowed )
                {
                    if( allow.equals( item ) )
                    {
                        return true;
                    }
                }
                return false;
            }
        };
    }

    public static <T> Specification<T> notNull()
    {
        return new Specification<T>()
        {
            @Override
            public boolean satisfiedBy( T item )
            {
                return item != null;
            }
        };
    }

    public static <FROM,TO> Specification<FROM> translate( final Function<FROM,TO> function, final Specification<? super TO> specification)
    {
        return new Specification<FROM>()
        {
            @Override
            public boolean satisfiedBy( FROM item )
            {
                return specification.satisfiedBy( function.map( item ) );
            }
        };
    }

    public static class AndSpecification<T> implements Specification<T>
    {
        private final Iterable<Specification<T>> specifications;

        private AndSpecification( Iterable<Specification<T>> specifications )
        {
            this.specifications = specifications;
        }

        public boolean satisfiedBy( T instance )
        {
            for( Specification<T> specification : specifications )
            {
                if( !specification.satisfiedBy( instance ) )
                {
                    return false;
                }
            }

            return true;
        }

        public AndSpecification<T> and(Specification<T>... specifications)
        {
            Iterable<Specification<T>> iterable = Iterables.iterable( specifications );
            Iterable<Specification<T>> flatten = Iterables.flatten( this.specifications, iterable );
            return Specifications.and( flatten );
        }

        public OrSpecification<T> or(Specification<T>... specifications)
        {
            return Specifications.or( Iterables.prepend( this, Iterables.iterable( specifications ) ) );
        }
    }

    public static class OrSpecification<T> implements Specification<T>
    {
        private final Iterable<Specification<T>> specifications;

        private OrSpecification( Iterable<Specification<T>> specifications )
        {
            this.specifications = specifications;
        }

        public boolean satisfiedBy( T instance )
        {
            for( Specification<T> specification : specifications )
            {
                if( specification.satisfiedBy( instance ) )
                {
                    return true;
                }
            }

            return false;
        }

        public AndSpecification<T> and(Specification<T>... specifications)
        {
            return Specifications.and( Iterables.prepend( this, Iterables.iterable( specifications ) ) );
        }

        public OrSpecification<T> or(Specification<T>... specifications)
        {
            Iterable<Specification<T>> iterable = Iterables.iterable( specifications );
            Iterable<Specification<T>> flatten = Iterables.flatten( this.specifications, iterable );
            return Specifications.or( flatten );
        }
    }
}
