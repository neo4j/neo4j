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
package org.neo4j.collection.primitive.hopscotch;

import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@SuppressWarnings( "unchecked" )
public class ClosingTablesTest
{
    @Test
    public void intCollectionsMustDelegateCloseToTable()
    {
        // Given
        Table table = mock( Table.class );
        AbstractIntHopScotchCollection coll = new AbstractIntHopScotchCollection( table )
        {
            @Override
            public boolean equals( Object other )
            {
                return false;
            }

            @Override
            public int hashCode()
            {
                return 0;
            }
        };

        // When
        coll.close();

        // Then
        verify( table ).close();
    }

    @Test
    public void longCollectionsMustDelegateCloseToTable()
    {
        // Given
        Table table = mock( Table.class );
        AbstractLongHopScotchCollection coll =
                new AbstractLongHopScotchCollection( table )
                {
                    @Override
                    public boolean equals( Object other )
                    {
                        return false;
                    }

                    @Override
                    public int hashCode()
                    {
                        return 0;
                    }
                };

        // When
        coll.close();

        // Then
        verify( table ).close();
    }
}
