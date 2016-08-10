/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.coreedge.core.state.storage;

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
            throws IOException
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
