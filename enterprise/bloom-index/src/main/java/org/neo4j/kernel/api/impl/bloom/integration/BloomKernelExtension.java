/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.bloom.integration;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.impl.bloom.BloomIndex;
import org.neo4j.kernel.api.impl.bloom.BloomIndexTransactionEventUpdater;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class BloomKernelExtension extends LifecycleAdapter
{
    private final File storeDir;
    private final Config config;
    private final FileSystemAbstraction fileSystemAbstraction;
    private GraphDatabaseService db;
    private Procedures procedures;
    private BloomIndex bloomIndex;
    private BloomIndexTransactionEventUpdater bloomIndexTransactionEventUpdater;

    public BloomKernelExtension( FileSystemAbstraction fileSystemAbstraction, File storeDir, Config config, GraphDatabaseService db, Procedures procedures )
    {
        this.storeDir = storeDir;
        this.config = config;
        this.fileSystemAbstraction = fileSystemAbstraction;
        this.db = db;
        this.procedures = procedures;
    }

    @Override
    public void init() throws IOException, ProcedureException
    {
        bloomIndex = new BloomIndex( fileSystemAbstraction, storeDir, config );
        bloomIndexTransactionEventUpdater = bloomIndex.getUpdater();
        db.registerTransactionEventHandler( bloomIndexTransactionEventUpdater );

        BloomNodeProcedure nodeProc = new BloomNodeProcedure( bloomIndex );
        BloomRelationshipProcedure relProc = new BloomRelationshipProcedure( bloomIndex );
        procedures.register( nodeProc );
        procedures.register( relProc );
    }

    @Override
    public void shutdown() throws Exception
    {
        db.unregisterTransactionEventHandler( bloomIndexTransactionEventUpdater );
        bloomIndex.close();
    }
}
