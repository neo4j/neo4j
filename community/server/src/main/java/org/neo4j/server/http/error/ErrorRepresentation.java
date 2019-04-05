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
package org.neo4j.server.http.error;

import java.util.List;

public class ErrorRepresentation
{
    private List<Error> errors;

    public List<Error> getErrors()
    {
        return errors;
    }

    public void setErrors( List<Error> errors )
    {
        this.errors = errors;
    }

    public static class Error
    {

        private String code;
        private String message;

        public Error( String code, String message )
        {
            this.code = code;
            this.message = message;
        }

        public String getCode()
        {
            return code;
        }

        public void setCode( String code )
        {
            this.code = code;
        }

        public String getMessage()
        {
            return message;
        }

        public void setMessage( String message )
        {
            this.message = message;
        }
    }
}
