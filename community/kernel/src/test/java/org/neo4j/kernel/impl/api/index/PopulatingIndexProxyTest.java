/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.impl.api.index;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import org.neo4j.kernel.api.schema.index.CapableIndexDescriptor;

import static org.mockito.Mockito.verify;

@RunWith( MockitoJUnitRunner.class )
public class PopulatingIndexProxyTest
{
    @Mock
    private CapableIndexDescriptor capableIndexDescriptor;
    @Mock
    private IndexPopulationJob indexPopulationJob;
    @Mock
    private MultipleIndexPopulator.IndexPopulation indexPopulation;
    private PopulatingIndexProxy populatingIndexProxy;

    @Before
    public void setUp()
    {
        populatingIndexProxy = new PopulatingIndexProxy( capableIndexDescriptor, indexPopulationJob, indexPopulation );
    }

    @Test
    public void cancelPopulationJobOnClose()
    {
        populatingIndexProxy.close();

        verify( indexPopulationJob ).cancelPopulation( indexPopulation );
    }

    @Test
    public void cancelPopulationJobOnDrop()
    {
        populatingIndexProxy.drop();

        verify( indexPopulationJob ).cancelPopulation( indexPopulation );
    }
}
