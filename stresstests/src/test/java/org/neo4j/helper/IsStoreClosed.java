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
package org.neo4j.helper;

import java.util.function.Predicate;

import org.neo4j.kernel.impl.util.UnsatisfiedDependencyException;

public class IsStoreClosed implements Predicate<Throwable>
{
    @Override
    public boolean test( Throwable ex )
    {

        if ( ex == null )
        {
            return false;
        }

        if ( ex instanceof UnsatisfiedDependencyException )
        {
            return true;
        }

        if ( ex instanceof IllegalStateException )
        {
            String message = ex.getMessage();
            return message.startsWith( "MetaDataStore for file " ) && message.endsWith( " is closed" );
        }

        return test( ex.getCause() );
    }
}
