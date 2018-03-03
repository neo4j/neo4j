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
package org.neo4j.kernel.impl.newapi;

import org.junit.Test;

import org.neo4j.function.ThrowingAction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.test.assertion.Assert;

import static org.mockito.Mockito.mock;

public class KernelTokenArgumentTest
{
    private KernelToken token = new KernelToken( mock( StoreReadLayer.class ), mock( KernelTransactionImplementation.class) );

    @Test
    public void labelGetOrCreateForName()
    {
        assertIllegalToken( () -> token.labelGetOrCreateForName( null ) );
        assertIllegalToken( () -> token.labelGetOrCreateForName( "" ) );
    }

    @Test
    public void propertyKeyGetOrCreateForName()
    {
        assertIllegalToken( () -> token.propertyKeyGetOrCreateForName( null ) );
        assertIllegalToken( () -> token.propertyKeyGetOrCreateForName( "" ) );
    }

    @Test
    public void relationshipTypeGetOrCreateForName()
    {
        assertIllegalToken( () -> token.relationshipTypeGetOrCreateForName( null ) );
        assertIllegalToken( () -> token.relationshipTypeGetOrCreateForName( "" ) );
    }

    private void assertIllegalToken( ThrowingAction<KernelException> f )
    {
        Assert.assertException( f, IllegalTokenNameException.class );
    }
}
