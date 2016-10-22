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
package org.neo4j.causalclustering.backup;

import java.io.File;
import java.util.LinkedList;

public class ArgsBuilder
{
    private LinkedList<String> args = new LinkedList<>(  );

    public static ArgsBuilder args()
    {
        return new ArgsBuilder();
    }

    public ArgsBuilder homeDir( File homeDir)
    {
        args.add( String.format( "--home-dir=%s", homeDir.getPath() ) );
        return this;
    }

    public ArgsBuilder database( String databaseName)
    {
        args.add( String.format( "--database=%s", databaseName ) );
        return this;
    }

    public ArgsBuilder seed( String seed)
    {
        args.add( String.format( "--cluster-seed=%s", seed ) );
        return this;
    }

    public ArgsBuilder config( File config)
    {
        args.add( String.format( "--config=%s", config.getPath() ) );
        return this;
    }

    public ArgsBuilder from( File from)
    {
        args.add( String.format( "--from=%s", from.getPath() ) );
        return this;
    }

    public ArgsBuilder force()
    {
        args.add( "--force=true" );
        return this;
    }

    public LinkedList<String> build()
    {
        return args;
    }

    public static String[] toArray( LinkedList<String> restoreOtherArgs )
    {
        return restoreOtherArgs.toArray( new String[restoreOtherArgs.size()] );
    }
}
