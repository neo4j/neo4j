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

import static org.neo4j.kernel.impl.api.index.SchemaIndexTestHelper.mockIndexContext;

import org.junit.Test;
import org.neo4j.test.DoubleLatch;

public class ContractCheckingIndexContextTest
{
    @Test( expected = /* THEN */ IllegalStateException.class )
    public void shouldNotCreateIndexTwice()
    {
        // GIVEN
        IndexContext inner = mockIndexContext();
        IndexContext outer = new ContractCheckingIndexContext( inner );

        // WHEN
        outer.create();
        outer.create();
    }

    @Test( expected = /* THEN */ IllegalStateException.class )
    public void shouldNotCloseIndexTwice()
    {
        // GIVEN
        IndexContext inner = mockIndexContext();
        IndexContext outer = new ContractCheckingIndexContext( inner );

        // WHEN
        outer.close();
        outer.close();
    }

    @Test( expected = /* THEN */ IllegalStateException.class )
    public void shouldNotDropIndexTwice()
    {
        // GIVEN
        IndexContext inner = mockIndexContext();
        IndexContext outer = new ContractCheckingIndexContext( inner );

        // WHEN
        outer.drop();
        outer.drop();
    }

    @Test( expected = /* THEN */ IllegalStateException.class )
    public void shouldNotDropAfterClose()
    {
        // GIVEN
        IndexContext inner = mockIndexContext();
        IndexContext outer = new ContractCheckingIndexContext( inner );

        // WHEN
        outer.close();
        outer.drop();
    }


    @Test
    public void shouldDropAfterCreate()
    {
        // GIVEN
        IndexContext inner = mockIndexContext();
        IndexContext outer = new ContractCheckingIndexContext( inner );

        // WHEN
        outer.create();
        outer.drop();
    }


    @Test
    public void shouldCloseAfterCreate()
    {
        // GIVEN
        IndexContext inner = mockIndexContext();
        IndexContext outer = new ContractCheckingIndexContext( inner );

        // WHEN
        outer.create();
        outer.close();
    }

    @Test( expected = /* THEN */ IllegalStateException.class )
    public void shouldNotCloseWhileCreating()
    {
        // GIVEN
        final DoubleLatch latch = new DoubleLatch();
        final IndexContext inner = new IndexContext.Adapter()
        {
            @Override
            public void create()
            {
                latch.startAndAwaitFinish();
            }
        };
        final IndexContext outer = new ContractCheckingIndexContext( inner );

        // WHEN
        new Thread( new Runnable()
        {
            @Override
            public void run()
            {
                outer.create();
            }
        } ).start();

        try
        {
            latch.awaitStart();
            outer.close();
        }
        finally
        {
            latch.finish();
        }
    }


    @Test( expected = /* THEN */ IllegalStateException.class )
    public void shouldNotDropWhileCreating()
    {
        // GIVEN
        final DoubleLatch latch = new DoubleLatch();
        final IndexContext inner = new IndexContext.Adapter()
        {
            @Override
            public void create()
            {
                latch.startAndAwaitFinish();
            }
        };
        final IndexContext outer = new ContractCheckingIndexContext( inner );

        // WHEN
        new Thread( new Runnable()
        {
            @Override
            public void run()
            {
                outer.create();
            }
        } ).start();

        try
        {
            latch.awaitStart();
            outer.drop();
        }
        finally
        {
            latch.finish();
        }
    }
}
