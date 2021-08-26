/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.ast.factory;

public interface SimpleEither<L, R>
{
    static <L, R> SimpleEither<L,R> left( L value )
    {
        return new EitherImpl( value, null );
    }

    static <L, R> SimpleEither<L,R> right( R value )
    {
        return new EitherImpl( null, value );
    }

    L getLeft();

    R getRight();
}

class EitherImpl<L, R> implements SimpleEither<L,R>
{
    private final L left;
    private final R right;

    EitherImpl( L left, R right )
    {
        if ( left == null && right == null )
        {
            throw new IllegalStateException( "no value set for Either" );
        }
        this.left = left;
        this.right = right;
    }

    @Override
    public L getLeft()
    {
        return left;
    }

    @Override
    public R getRight()
    {
        return right;
    }
}
