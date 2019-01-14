/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
