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
package org.neo4j.collection.primitive;

import java.util.Arrays;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertSame;
import static org.neo4j.collection.primitive.PrimitiveSortedArraySet.union;

@RunWith( Parameterized.class )
public class PrimitiveSortedArraySetUnionTest
{
    @Parameterized.Parameters( name = "{0}" )
    public static Iterable<Object[]> parameters()
    {
        return Arrays.asList(
                lhs( 1, 2, 3 ).rhs( 1, 2, 3 ).expectLhs(),
                lhs( 1, 2, 3 ).rhs( 1 ).expectLhs(),
                lhs( 1, 2, 3 ).rhs( 2 ).expectLhs(),
                lhs( 1, 2, 3 ).rhs( 3 ).expectLhs(),
                lhs( 1 ).rhs( 1, 2, 3 ).expectRhs(),
                lhs( 2 ).rhs( 1, 2, 3 ).expectRhs(),
                lhs( 3 ).rhs( 1, 2, 3 ).expectRhs(),
                lhs( 1, 2, 3 ).rhs( 4, 5, 6 ).expect( 1, 2, 3, 4, 5, 6 ),
                lhs( 1, 3, 5 ).rhs( 2, 4, 6 ).expect( 1, 2, 3, 4, 5, 6 ),
                lhs( 1, 2, 3, 5 ).rhs( 2, 4, 6 ).expect( 1, 2, 3, 4, 5, 6 ),
                lhs( 2, 3, 4, 7, 8, 9, 12, 16, 19 ).rhs( 4, 6, 9, 11, 12, 15 )
                        .expect( 2, 3, 4, 6, 7, 8, 9, 11, 12, 15, 16, 19 )
        );
    }

    private final int[] lhs;
    private final int[] rhs;
    private final int[] expected;

    public PrimitiveSortedArraySetUnionTest( Input input )
    {
        this.lhs = input.lhs;
        this.rhs = input.rhs;
        this.expected = input.expected;
    }

    @Test
    public void testMerge() throws Exception
    {
        int[] actual = union( lhs, rhs );
        if ( lhs == expected || rhs == expected )
        {
            assertSame( expected, actual );
        }
        else
        {
            assertArrayEquals( expected, actual );
        }
    }

    private static Input.Lhs lhs( int... lhs )
    {
        return new Input.Lhs( lhs );
    }

    static class Input
    {
        final int[] lhs;
        final int[] rhs;
        final int[] expected;

        Input( int[] lhs, int[] rhs, int[] expected )
        {
            this.lhs = lhs;
            this.rhs = rhs;
            this.expected = expected;
        }

        @Override
        public String toString()
        {
            return String.format(
                    "{lhs=%s, rhs=%s, expected=%s}",
                    Arrays.toString( lhs ),
                    Arrays.toString( rhs ),
                    Arrays.toString( expected ) );
        }

        static class Lhs
        {
            final int[] lhs;

            Lhs( int[] lhs )
            {
                this.lhs = lhs;
            }

            Rhs rhs( int... rhs )
            {
                return new Rhs( lhs, rhs );
            }
        }

        static class Rhs
        {
            final int[] lhs;
            final int[] rhs;

            Rhs( int[] lhs, int[] rhs )
            {

                this.lhs = lhs;
                this.rhs = rhs;
            }

            Object[] expect( int... expected )
            {
                return new Object[] {new Input( lhs, rhs, expected )};
            }

            Object[] expectLhs()
            {
                return new Object[] {new Input( lhs, rhs, lhs )};
            }

            Object[] expectRhs()
            {
                return new Object[] {new Input( lhs, rhs, rhs )};
            }
        }
    }
}
