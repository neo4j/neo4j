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
package org.neo4j.kernel.impl.index.schema;

import org.junit.Test;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.impl.api.integrationtest.KernelIntegrationTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.api.schema.SchemaDescriptorFactory.forLabel;

public class IndexCreateIT extends KernelIntegrationTest
{
    @Test
    public void shouldCreateIndexWithSpecificExistingProviderName() throws KernelException
    {
        int labelId = 0;
        for ( GraphDatabaseSettings.SchemaIndex indexSetting : GraphDatabaseSettings.SchemaIndex.values() )
        {
            // given
            SchemaWrite schemaWrite = schemaWriteInNewTransaction();
            String provider = indexSetting.providerName();
            LabelSchemaDescriptor descriptor = forLabel( labelId++, 0 );
            schemaWrite.indexCreate( descriptor, provider );

            // when
            commit();

            // then
            assertEquals( provider, indexingService.getIndexProxy( descriptor ).getProviderDescriptor().name() );
        }
    }

    @Test
    public void shouldFailOnCreateIndexWithNonExistentProviderName() throws KernelException
    {
        // given
        SchemaWrite schemaWrite = schemaWriteInNewTransaction();

        // when
        try
        {
            schemaWrite.indexCreate( forLabel( 0, 0 ), "something-completely-different" );
            fail( "Should have failed" );
        }
        catch ( IllegalArgumentException e )
        {
            // then good
        }
    }
}
