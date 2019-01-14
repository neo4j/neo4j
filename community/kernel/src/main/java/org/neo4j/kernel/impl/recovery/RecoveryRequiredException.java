/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.recovery;

import org.neo4j.helpers.Strings;

public class RecoveryRequiredException extends Exception
{
    private static final String MESSAGE =
            Strings.joinAsLines( "Active logical log detected, this might be a source of inconsistencies.", "Please recover database.",
                    "To perform recovery please start database in single mode and perform clean shutdown." );

    RecoveryRequiredException()
    {
        super( MESSAGE );
    }
}
