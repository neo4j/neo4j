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
package org.neo4j.kernel.api.security;

import org.neo4j.exceptions.KernelException;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.security.provider.SecurityProvider;
import org.neo4j.logging.Log;

public abstract class SecurityModule implements SecurityProvider
{
    protected final void registerProcedure( GlobalProcedures globalProcedures, Log log, Class procedureClass, String warning )
    {
        try
        {
            globalProcedures.registerProcedure( procedureClass, true, warning );
        }
        catch ( KernelException e )
        {
            String message = "Failed to register security procedures: " + e.getMessage();
            log.error( message, e );
            throw new RuntimeException( message, e );
        }
    }

    public abstract void setup();
}
