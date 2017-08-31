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
package org.neo4j.kernel.api.impl.fulltext.integrations.bloom;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.impl.fulltext.FulltextHelperFactory;
import org.neo4j.kernel.api.impl.fulltext.FulltextHelperProvider;
import org.neo4j.kernel.api.impl.fulltext.LuceneFulltextHelper;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class BloomKernelExtension extends LifecycleAdapter
{
    private final File storeDir;
    private final Config config;
    private final FileSystemAbstraction fileSystemAbstraction;
    private final GraphDatabaseService db;
    private final Procedures procedures;
    private FulltextHelperFactory fulltextHelperFactory;

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
        FulltextHelperProvider provider = FulltextHelperProvider.instance( db );
        fulltextHelperFactory = new FulltextHelperFactory( fileSystemAbstraction, storeDir, config );
        LuceneFulltextHelper nodes = fulltextHelperFactory.createFulltextHelper( "bloomNodes", FulltextHelperFactory.FULLTEXT_HELPER_TYPE.NODES );
        LuceneFulltextHelper relationships =
                fulltextHelperFactory.createFulltextHelper( "bloomRelationships", FulltextHelperFactory.FULLTEXT_HELPER_TYPE.RELATIONSHIPS );
        provider.register( nodes );
        provider.register( relationships );

        procedures.register( new BloomProcedure( "Nodes", nodes ) );
        procedures.register( new BloomProcedure( "Relationships", relationships ) );
    }

    @Override
    public void shutdown() throws Exception
    {
        FulltextHelperProvider.instance( db ).close();
    }
}
