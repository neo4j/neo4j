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
package org.neo4j.server.rest.management.repr;

import static org.neo4j.server.rest.repr.ValueRepresentation.string;

import java.util.ArrayList;

import javax.management.ObjectName;

import org.neo4j.server.rest.repr.ListRepresentation;
import org.neo4j.server.rest.repr.ObjectRepresentation;
import org.neo4j.server.rest.repr.ValueRepresentation;

public class JmxDomainRepresentation extends ObjectRepresentation
{

    protected ArrayList<JmxMBeanRepresentation> beans = new ArrayList<JmxMBeanRepresentation>();
    protected String domainName;

    public JmxDomainRepresentation( String name )
    {
        super( "jmxDomain" );
        this.domainName = name;
    }

    @Mapping( "domain" )
    public ValueRepresentation getDomainName()
    {
        return string( this.domainName );
    }

    @Mapping( "beans" )
    public ListRepresentation getBeans()
    {
        return new ListRepresentation( "bean", beans );
    }

    public void addBean( ObjectName bean )
    {
        beans.add( new JmxMBeanRepresentation( bean ) );
    }
}
