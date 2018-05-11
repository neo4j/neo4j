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
package org.neo4j.kernel.impl.api.store;


import org.junit.Test;

import org.neo4j.internal.kernel.api.IndexReference;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.internal.kernel.api.IndexCapability.NO_CAPABILITY;
import static org.neo4j.kernel.api.index.IndexProvider.UNDECIDED;

public class DefaultCapableIndexReferenceTest
{

    private static final int LABEL = 42;
    private static final int PROPERTY = 1337;

    @Test
    public void capableIndexReferenceShouldBeEquivalentToNormalIndexReference()
    {
        // Given
        DefaultCapableIndexReference capable =
                new DefaultCapableIndexReference( false, NO_CAPABILITY, UNDECIDED, LABEL, PROPERTY );
        IndexReference incapable = DefaultIndexReference.general( LABEL, PROPERTY );

        // Then
        assertThat( capable, equalTo( incapable ));
        assertThat( capable.hashCode(), equalTo( incapable.hashCode() ));
    }

}
