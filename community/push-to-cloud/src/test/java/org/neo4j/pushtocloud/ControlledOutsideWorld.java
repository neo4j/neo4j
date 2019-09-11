/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.pushtocloud;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.commandline.admin.NullOutsideWorld;
import org.neo4j.io.fs.FileSystemAbstraction;

import static org.neo4j.io.NullOutputStream.NULL_OUTPUT_STREAM;

class ControlledOutsideWorld extends NullOutsideWorld
{
    private final List<String> promptResponses = new ArrayList<>();
    private final List<char[]> passwordResponses = new ArrayList<>();
    private final FileSystemAbstraction fs;
    private final PrintStream nullOutputStream = new PrintStream( NULL_OUTPUT_STREAM );
    private int promptResponseCursor;
    private int passwordResponseCursor;

    ControlledOutsideWorld( FileSystemAbstraction fs )
    {
        this.fs = fs;
    }

    ControlledOutsideWorld withPromptResponse( String line )
    {
        promptResponses.add( line );
        return this;
    }

    ControlledOutsideWorld withPasswordResponse( char[] password )
    {
        passwordResponses.add( password );
        return this;
    }

    @Override
    public String promptLine( String fmt, Object... args )
    {
        if ( promptResponseCursor < promptResponses.size() )
        {
            return promptResponses.get( promptResponseCursor++ );
        }
        return "";
    }

    @Override
    public char[] promptPassword( String fmt, Object... args )
    {
        if ( passwordResponseCursor < passwordResponses.size() )
        {
            return passwordResponses.get( passwordResponseCursor++ );
        }
        return new char[0];
    }

    @Override
    public FileSystemAbstraction fileSystem()
    {
        return fs;
    }

    @Override
    public PrintStream outStream()
    {
        return nullOutputStream;
    }
}
