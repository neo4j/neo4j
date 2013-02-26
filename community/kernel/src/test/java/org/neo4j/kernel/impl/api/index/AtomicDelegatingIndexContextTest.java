/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.index;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.junit.Test;

public class AtomicDelegatingIndexContextTest
{
    @Test
    public void shouldBeAbleToSwitchDelegate() throws Exception
    {
        // GIVEN
        IndexContext actual = mock( IndexContext.class );
        IndexContext other = mock( IndexContext.class );
        AtomicDelegatingIndexContext delegate = new AtomicDelegatingIndexContext( actual );

        // WHEN
        delegate.setDelegate( other );
        delegate.drop();

        // THEN
        verify( other ).drop();
    }
}
