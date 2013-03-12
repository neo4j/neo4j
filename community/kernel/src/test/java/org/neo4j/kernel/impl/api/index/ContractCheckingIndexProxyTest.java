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

import static org.neo4j.kernel.impl.api.index.SchemaIndexTestHelper.mockIndexProxy;

import org.junit.Test;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.test.DoubleLatch;

public class ContractCheckingIndexProxyTest
{
    @Test( expected = /* THEN */ IllegalStateException.class )
    public void shouldNotCreateIndexTwice()
    {
        // GIVEN
        IndexProxy inner = mockIndexProxy();
        IndexProxy outer = new ContractCheckingIndexProxy( inner );

        // WHEN
        outer.create();
        outer.create();
    }

    @Test( expected = /* THEN */ IllegalStateException.class )
    public void shouldNotCloseIndexTwice()
    {
        // GIVEN
        IndexProxy inner = mockIndexProxy();
        IndexProxy outer = new ContractCheckingIndexProxy( inner );

        // WHEN
        outer.close();
        outer.close();
    }

    @Test( expected = /* THEN */ IllegalStateException.class )
    public void shouldNotDropIndexTwice()
    {
        // GIVEN
        IndexProxy inner = mockIndexProxy();
        IndexProxy outer = new ContractCheckingIndexProxy( inner );

        // WHEN
        outer.drop();
        outer.drop();
    }

    @Test( expected = /* THEN */ IllegalStateException.class )
    public void shouldNotDropAfterClose()
    {
        // GIVEN
        IndexProxy inner = mockIndexProxy();
        IndexProxy outer = new ContractCheckingIndexProxy( inner );

        // WHEN
        outer.close();
        outer.drop();
    }


    @Test
    public void shouldDropAfterCreate()
    {
        // GIVEN
        IndexProxy inner = mockIndexProxy();
        IndexProxy outer = new ContractCheckingIndexProxy( inner );

        // WHEN
        outer.create();

        // PASS
        outer.drop();
    }


    @Test
    public void shouldCloseAfterCreate()
    {
        // GIVEN
        IndexProxy inner = mockIndexProxy();
        IndexProxy outer = new ContractCheckingIndexProxy( inner );

        // WHEN
        outer.create();

        // PASS
        outer.close();
    }


    @Test(expected = IllegalStateException.class)
    public void shouldNotUpdateBeforeCreate()
    {
        // GIVEN
        IndexProxy inner = mockIndexProxy();
        IndexProxy outer = new ContractCheckingIndexProxy( inner );

        // WHEN
        outer.update( null );
    }

    @Test(expected = IllegalStateException.class)
    public void shouldNotUpdateAfterClose()
    {
        // GIVEN
        IndexProxy inner = mockIndexProxy();
        IndexProxy outer = new ContractCheckingIndexProxy( inner );

        // WHEN
        outer.create();
        outer.close();
        outer.update( null );
    }

    @Test(expected = IllegalStateException.class)
    public void shouldNotForceBeforeCreate()
    {
        // GIVEN
        IndexProxy inner = mockIndexProxy();
        IndexProxy outer = new ContractCheckingIndexProxy( inner );

        // WHEN
        outer.force();
    }

    @Test(expected = IllegalStateException.class)
    public void shouldNotForceAfterClose()
    {
        // GIVEN
        IndexProxy inner = mockIndexProxy();
        IndexProxy outer = new ContractCheckingIndexProxy( inner );

        // WHEN
        outer.create();
        outer.close();
        outer.force();
    }

    @Test( expected = /* THEN */ IllegalStateException.class )
    public void shouldNotCloseWhileCreating()
    {
        // GIVEN
        final DoubleLatch latch = new DoubleLatch();
        final IndexProxy inner = new IndexProxy.Adapter()
        {
            @Override
            public void create()
            {
                latch.startAndAwaitFinish();
            }
        };
        final IndexProxy outer = new ContractCheckingIndexProxy( inner );

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
        final IndexProxy inner = new IndexProxy.Adapter()
        {
            @Override
            public void create()
            {
                latch.startAndAwaitFinish();
            }
        };
        final IndexProxy outer = new ContractCheckingIndexProxy( inner );

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


    @Test( expected = /* THEN */ IllegalStateException.class )
    public void shouldNotCloseWhileUpdating()
    {
        // GIVEN
        final DoubleLatch latch = new DoubleLatch();
        final IndexProxy inner = new IndexProxy.Adapter()
        {
            @Override
            public void update(Iterable<NodePropertyUpdate> updates)
            {
                latch.startAndAwaitFinish();
            }
        };
        final IndexProxy outer = new ContractCheckingIndexProxy( inner );
        outer.create();

        // WHEN
        new Thread( new Runnable()
        {
            @Override
            public void run()
            {
                outer.update( null );
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
    public void shouldNotCloseWhileForcing()
    {
        // GIVEN
        final DoubleLatch latch = new DoubleLatch();
        final IndexProxy inner = new IndexProxy.Adapter()
        {
            @Override
            public void force()
            {
                latch.startAndAwaitFinish();
            }
        };
        final IndexProxy outer = new ContractCheckingIndexProxy( inner );
        outer.create();

        // WHEN
        new Thread( new Runnable()
        {
            @Override
            public void run()
            {
                outer.force();
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
}
