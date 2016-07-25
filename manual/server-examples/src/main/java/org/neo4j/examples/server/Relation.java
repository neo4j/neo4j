/*
 * Licensed to Neo Technology under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Neo Technology licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.neo4j.examples.server;

public class Relation
{
    public static final String OUT = "out";
    public static final String IN = "in";
    public static final String BOTH = "both";
    private String type;
    private String direction;

    public String toJsonCollection()
    {
        StringBuilder sb = new StringBuilder();
        sb.append( "{ " );
        sb.append( " \"type\" : \"" + type + "\"" );
        if ( direction != null )
        {
            sb.append( ", \"direction\" : \"" + direction + "\"" );
        }
        sb.append( " }" );
        return sb.toString();
    }

    public Relation( String type, String direction )
    {
        setType( type );
        setDirection( direction );
    }

    public Relation( String type )
    {
        this( type, null );
    }

    public void setType( String type )
    {
        this.type = type;
    }

    public void setDirection( String direction )
    {
        this.direction = direction;
    }
}
