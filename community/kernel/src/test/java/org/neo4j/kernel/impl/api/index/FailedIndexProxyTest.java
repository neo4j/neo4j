/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.junit.Test;

import java.io.IOException;

import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.impl.util.StringLogger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class FailedIndexProxyTest
{
    @Test
    public void shouldLogReasonForDroppingIndex() throws IOException
    {
        // given
        StringLogger log = mock( StringLogger.class );

        // when
        new FailedIndexProxy( new IndexDescriptor( 0, 0 ), new SchemaIndexProvider.Descriptor( "foo", "bar" ), "foo",
                mock( IndexPopulator.class ), IndexPopulationFailure.failure( "it broke" ), log ).drop();

        // then
        verify(log).info( "FailedIndexProxy#drop index on foo dropped due to:\nit broke" );
    }

}
