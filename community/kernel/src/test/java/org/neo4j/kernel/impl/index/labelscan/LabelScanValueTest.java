/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.index.labelscan;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LabelScanValueTest
{
    @Test
    public void shouldAddBits()
    {
        // GIVEN
        LabelScanValue value = new LabelScanValue();
        value.bits = 0b0000__1000_0100__0010_0001;

        // WHEN
        LabelScanValue other = new LabelScanValue();
        other.bits = 0b1100__0100_0100__0100_0100;
        value.add( other );

        // THEN
        assertEquals( 0b1100__1100_0100__0110_0101, value.bits );
    }

    @Test
    public void shouldRemoveBits()
    {
        // GIVEN
        LabelScanValue value = new LabelScanValue();
        value.bits = 0b1100__1000_0100__0010_0001;

        // WHEN
        LabelScanValue other = new LabelScanValue();
        other.bits = 0b1000__0100_0100__0100_0100;
        value.remove( other );

        // THEN
        assertEquals( 0b0100__1000_0000__0010_0001, value.bits );
    }
}
