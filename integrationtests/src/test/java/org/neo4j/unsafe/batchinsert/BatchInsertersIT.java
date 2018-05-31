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

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.api.impl.schema.NativeLuceneFusionIndexProviderFactory20;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.test.rule.TestDirectory;

import static org.neo4j.unsafe.batchinsert.BatchInserters.inserter;

public class BatchInsertersIT
{

    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    @Test
    public void shouldStartBatchInserterWithRealIndexProvider() throws Exception
    {
        BatchInserter inserter = inserter( testDirectory.graphDbDir(), MapUtil.stringMap(), getKernelExtensions() );
        //If we didn't throw, all good!
        inserter.shutdown();
    }

    private Iterable<KernelExtensionFactory<?>> getKernelExtensions()
    {
        return Iterables.asIterable( new NativeLuceneFusionIndexProviderFactory20() );
    }
}
