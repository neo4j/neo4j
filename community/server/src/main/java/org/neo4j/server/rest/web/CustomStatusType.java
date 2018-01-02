/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.server.rest.web;

import javax.ws.rs.core.Response;

import static javax.ws.rs.core.Response.Status.Family;

public class CustomStatusType implements Response.StatusType
{
    public static Response.StatusType UNPROCESSABLE = new CustomStatusType( 422, "Unprocessable Entity" );
    public static Response.StatusType TOO_MANY = new CustomStatusType( 429, "Too Many Requests" );

    private final int code;
    private final String reason;
    private final Family family;

    public CustomStatusType( int code, String reason )
    {
        this.code = code;
        this.reason = reason;
        switch(code/100) {
            case 1: this.family = Family.INFORMATIONAL; break;
            case 2: this.family = Family.SUCCESSFUL; break;
            case 3: this.family = Family.REDIRECTION; break;
            case 4: this.family = Family.CLIENT_ERROR; break;
            case 5: this.family = Family.SERVER_ERROR; break;
            default: this.family = Family.OTHER; break;
        }
    }

    @Override
    public int getStatusCode()
    {
        return code;
    }

    @Override
    public Family getFamily()
    {
        return family;
    }

    @Override
    public String getReasonPhrase()
    {
        return reason;
    }
}
