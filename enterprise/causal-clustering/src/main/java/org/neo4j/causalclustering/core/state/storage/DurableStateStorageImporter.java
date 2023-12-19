/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core.state.storage;

import java.io.File;
import java.io.IOException;
import java.util.function.Supplier;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.logging.LogProvider;

public class DurableStateStorageImporter<STATE> extends DurableStateStorage<STATE>
{
    public DurableStateStorageImporter( FileSystemAbstraction fileSystemAbstraction, File stateDir, String name,
                                        StateMarshal<STATE> marshal, int numberOfEntriesBeforeRotation,
                                        Supplier<DatabaseHealth> databaseHealthSupplier, LogProvider logProvider )
    {
        super( fileSystemAbstraction, stateDir, name, marshal, numberOfEntriesBeforeRotation, logProvider );
    }

    public void persist( STATE state ) throws IOException
    {
        super.persistStoreData( state );
        super.switchStoreFile();
        super.persistStoreData( state );
    }
}
