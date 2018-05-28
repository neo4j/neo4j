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
