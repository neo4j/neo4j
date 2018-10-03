/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.unsafe.batchinsert;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.index.schema.GenericNativeIndexProviderFactory;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.default_schema_provider;
import static org.neo4j.unsafe.batchinsert.BatchInserters.inserter;

@ExtendWith( TestDirectoryExtension.class )
class BatchInsertersIT
{

    @Inject
    private TestDirectory testDirectory;

    @Test
    void shouldStartBatchInserterWithRealIndexProvider() throws Exception
    {
        BatchInserter inserter = inserter( testDirectory.databaseDir(), getConfig(), getKernelExtensions() );
        inserter.shutdown();
    }

    private static Map<String,String> getConfig()
    {
        return MapUtil.stringMap( default_schema_provider.name(), GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10.providerName() );
    }

    private static Iterable<KernelExtensionFactory<?>> getKernelExtensions()
    {
        return Iterables.asIterable( new GenericNativeIndexProviderFactory() );
    }
}
