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
package org.neo4j.util;

import org.junit.ClassRule;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.commandline.admin.AdminTool;
import org.neo4j.io.proc.ProcessUtil;
import org.neo4j.test.ProcessStreamHandler;
import org.neo4j.test.rule.TestDirectory;

public class JvmRunner
{
    @ClassRule
    public static final TestDirectory testDirectory = TestDirectory.testDirectory();

    public static int runBackupToolFromOtherJvmToGetExitCode( String... args ) throws Exception
    {
        return runBackupToolFromOtherJvmToGetExitCode( testDirectory.absolutePath(), args );
    }

    public static int runBackupToolFromOtherJvmToGetExitCode( File neo4jHome, String... args ) throws Exception
    {
        List<String> allArgs =
                new ArrayList<>( Arrays.asList( ProcessUtil.getJavaExecutable().toString(), "-cp", ProcessUtil.getClassPath(), AdminTool.class.getName() ) );
        allArgs.add( "backup" );
        allArgs.addAll( Arrays.asList( args ) );

        Process process = Runtime.getRuntime().exec( allArgs.toArray( new String[allArgs.size()] ), new String[]{"NEO4J_HOME=" + neo4jHome.getAbsolutePath()} );
        return new ProcessStreamHandler( process, true ).waitForResult();
    }
}
