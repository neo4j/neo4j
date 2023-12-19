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
package org.neo4j.helper;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileUtils;

import static java.lang.System.getenv;

public class StressTestingHelper
{
    private StressTestingHelper()
    {
    }

    public static File ensureExistsAndEmpty( File directory ) throws IOException
    {
        FileUtils.deleteRecursively( directory );

        if ( !directory.mkdirs() )
        {
            throw new RuntimeException( "Could not create directory: " + directory.getAbsolutePath() );
        }
        return directory;
    }

    public static String fromEnv( String environmentVariableName, String defaultValue )
    {
        String environmentVariableValue = getenv( environmentVariableName );
        return environmentVariableValue == null ? defaultValue : environmentVariableValue;
    }
}
