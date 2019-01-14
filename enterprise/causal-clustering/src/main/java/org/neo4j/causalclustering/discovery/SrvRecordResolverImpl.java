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
