/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.com;

public interface ComExceptionHandler
{
    static ComExceptionHandler NO_OP = new ComExceptionHandler()
    {
        @Override
        public RuntimeException handle( ComException exception )
        {
            return exception;
        }
    };

    /**
     * Adds additional handling of a {@link ComException} when it occurs on the client side in a client->server
     * communication scenario.
     *
     * @param exception {@link ComException} describing the failure.
     * @return {@link RuntimeException} describing the failure to the user. This transaction will
     * fail the user transaction and this exception will be the exception thrown out to the user.
     */
    RuntimeException handle( ComException exception );
}
