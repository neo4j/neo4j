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
package org.neo4j.com.storecopy;

import java.io.File;
import java.nio.file.StandardCopyOption;
import java.util.function.Function;
import java.util.stream.Stream;

@FunctionalInterface
public interface MoveAfterCopy
{
    void move( Stream<FileMoveAction> moves, File fromDirectory, Function<File, File> destinationFunction ) throws
            Exception;

    static MoveAfterCopy moveReplaceExisting()
    {
        return ( moves, fromDirectory, destinationFunction ) ->
        {
            Iterable<FileMoveAction> itr = moves::iterator;
            for ( FileMoveAction move : itr )
            {
                move.move( destinationFunction.apply( move.file() ), StandardCopyOption.REPLACE_EXISTING );
            }
        };
    }
}
