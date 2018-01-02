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
package org.neo4j.harness.internal;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.function.Function;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileUtils;

/**
 * Manages user-defined cypher fixtures that can be exercised against the server.
 */
public class Fixtures
{
    private final List<String> fixtureStatements = new LinkedList<>();
    private final List<Function<GraphDatabaseService, Void>> fixtureFunctions = new LinkedList<>();

    private final String cypherSuffix = "cyp";

    private final FileFilter cypherFileOrDirectoryFilter = new FileFilter()
    {
        @Override
        public boolean accept( File file )
        {
            if(file.isDirectory())
            {
                return true;
            }
            String[] split = file.getName().split( "\\." );
            String suffix = split[split.length-1];
            return suffix.equals( cypherSuffix );
        }
    };

    public void add( File fixturePath )
    {
        try
        {
            if(fixturePath.isDirectory())
            {
                for ( File file : fixturePath.listFiles( cypherFileOrDirectoryFilter ) )
                {
                    add(file);
                }
                return;
            }
            add( FileUtils.readTextFile( fixturePath, Charset.forName( "UTF-8" ) ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Unable to read fixture file '"+fixturePath.getAbsolutePath()+"': " + e.getMessage(), e );
        }
    }

    public void add( String statement )
    {
        if(statement.trim().length() > 0)
        {
            fixtureStatements.add( statement );
        }
    }


    public void add( Function<GraphDatabaseService,Void> fixtureFunction )
    {
        fixtureFunctions.add( fixtureFunction );
    }

    public void applyTo( InProcessServerControls controls )
    {
        GraphDatabaseService db = controls.graph();
        for ( String fixtureStatement : fixtureStatements )
        {
            try( Transaction tx = db.beginTx() )
            {
                db.execute( fixtureStatement );
                tx.success();
            }
        }
        for ( Function<GraphDatabaseService, Void> fixtureFunction : fixtureFunctions )
        {
            fixtureFunction.apply( db );
        }
    }
}
