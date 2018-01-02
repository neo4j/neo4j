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
package org.neo4j.shell;

import java.io.Serializable;

public class Welcome implements Serializable
{
    private final String message;
    private final Serializable id;
    private final String prompt;
    
    public Welcome( String message, Serializable id, String prompt )
    {
        this.message = message;
        this.id = id;
        this.prompt = prompt;
    }
    
    public String getMessage()
    {
        return message;
    }
    
    public Serializable getId()
    {
        return id;
    }
    
    public String getPrompt()
    {
        return prompt;
    }
}
