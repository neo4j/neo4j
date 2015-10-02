/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.io;

/**
 * There are more than one way to think of the short-system name increments for bytes.
 *
 * One system is SI (International System of Units) which works in increments of 1.000.
 * The other system is the EIC (International Electrotechnical Commission) standard, which uses increments of 1.024.
 * The latter system is sometimes also known as the binary system, and has been accepted as part of the International System of Quantities.
 * Therefor, the EIC system is recommended as the default choice when communicating quantities of information.
 */
public enum ByteUnitSystem
{
    SI( 1000 ),
    EIC( 1024 );

    private final long increment;

    ByteUnitSystem( long increment )
    {
        this.increment = increment;
    }

    /**
     * Compute the increment factor from the given power.
     * <p/>
     * Giving zero always produces 1. Giving 1 will produce 1000 or 1024, for SI and EIC respectively, and so on.
     */
    public long factorFromPower( long power )
    {
        if ( power == 0 )
        {
            return 1;
        }
        long product = increment;
        for ( int i = 0; i < power - 1; i++ )
        {
            product = product * increment;
        }
        return product;
    }
}
