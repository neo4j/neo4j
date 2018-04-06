/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.discovery;

import java.util.Enumeration;
import java.util.Iterator;
import java.util.Optional;
import java.util.Properties;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import javax.naming.directory.InitialDirContext;

public class SrvRecordResolverImpl extends SrvRecordResolver
{
    private final String[] SRV_RECORDS = {"SRV"};
    private final String SRV_ATTR = "srv";

    private Optional<InitialDirContext> _idc = Optional.empty();

    public Stream<SrvRecord> resolveSrvRecord( String url ) throws NamingException
    {
        Attributes attrs = _idc.orElseGet( this::setIdc ).getAttributes( url, SRV_RECORDS );

        return enumerationAsStream( (NamingEnumeration<String>) attrs.get( SRV_ATTR ).getAll() ).map( SrvRecord::parse );
    }

    private synchronized InitialDirContext setIdc()
    {
        return _idc.orElseGet( () ->
        {
            Properties env = new Properties();
            env.put( Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.dns.DnsContextFactory" );
            try
            {
                _idc = Optional.of( new InitialDirContext( env ) );
                return _idc.get();
            }
            catch ( NamingException e )
            {
                throw new RuntimeException( e );
            }
        } );
    }

    private static <T> Stream<T> enumerationAsStream( Enumeration<T> e )
    {
        return StreamSupport.stream( Spliterators.spliteratorUnknownSize( new Iterator<T>()
        {
            public T next()
            {
                return e.nextElement();
            }

            public boolean hasNext()
            {
                return e.hasMoreElements();
            }
        }, Spliterator.ORDERED ), false );
    }
}
