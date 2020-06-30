/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.harness.internal;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

/**
 * Manages user-defined cypher fixtures that can be exercised against the server.
 */
public class Fixtures
{
    private final List<String> fixtureStatements = new ArrayList<>();
    private final List<Function<GraphDatabaseService, Void>> fixtureFunctions = new ArrayList<>();

    private static final String cypherSuffix = "cyp";

    private final DirectoryStream.Filter<Path> cypherFileOrDirectoryFilter = file ->
    {
        if ( Files.isDirectory( file ) )
        {
            return true;
        }
        String[] split = file.getFileName().toString().split( "\\." );
        String suffix = split[split.length - 1];
        return suffix.equals( cypherSuffix );
    };

    public void add( Path fixturePath )
    {
        try
        {
            if ( Files.isDirectory( fixturePath ) )
            {
                try ( DirectoryStream<Path> paths = Files.newDirectoryStream( fixturePath, cypherFileOrDirectoryFilter ) )
                {
                    paths.forEach( this::add );
                }
                return;
            }
            add( Files.readString( fixturePath ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Unable to read fixture file '" + fixturePath.toAbsolutePath() + "': " + e.getMessage(), e );
        }
    }

    public void add( String statement )
    {
        if ( !statement.isBlank() )
        {
            fixtureStatements.add( statement );
        }
    }

    public void add( Function<GraphDatabaseService,Void> fixtureFunction )
    {
        fixtureFunctions.add( fixtureFunction );
    }

    void applyTo( InProcessNeo4j controls )
    {
        GraphDatabaseService db = controls.defaultDatabaseService();
        for ( String fixtureStatement : fixtureStatements )
        {
            try ( Transaction tx = db.beginTx() )
            {
                tx.execute( fixtureStatement );
                tx.commit();
            }
        }
        for ( Function<GraphDatabaseService,Void> fixtureFunction : fixtureFunctions )
        {
            fixtureFunction.apply( db );
        }
    }
}
