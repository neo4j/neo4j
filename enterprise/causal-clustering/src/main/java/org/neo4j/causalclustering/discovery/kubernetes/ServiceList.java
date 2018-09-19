/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.causalclustering.discovery.kubernetes;

import java.util.List;

/**
 * See <a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.11/#servicelist-v1-core">ServiceList</a>
 */
public class ServiceList extends KubernetesType
{
    private List<Service> items;

    public ServiceList()
    {
    }

    public List<Service> items()
    {
        return items;
    }

    public void setItems( List<Service> items )
    {
        this.items = items;
    }

    @Override
    public <T> T handle( Visitor<T> visitor )
    {
        return visitor.visit( this );
    }

    public static class Service
    {
        private ObjectMetadata metadata;
        private ServiceSpec spec;

        public Service()
        {
        }

        public ObjectMetadata metadata()
        {
            return metadata;
        }

        public ServiceSpec spec()
        {
            return spec;
        }

        public void setMetadata( ObjectMetadata metadata )
        {
            this.metadata = metadata;
        }

        public void setSpec( ServiceSpec spec )
        {
            this.spec = spec;
        }

        public static class ServiceSpec
        {
            private List<ServicePort> ports;

            public ServiceSpec()
            {
            }

            public List<ServicePort> ports()
            {
                return ports;
            }

            public void setPorts( List<ServicePort> ports )
            {
                this.ports = ports;
            }

            public static class ServicePort
            {
                private String name;
                private int port;
                private String protocol;

                public ServicePort()
                {
                }

                public String name()
                {
                    return name;
                }

                public int port()
                {
                    return port;
                }

                public String protocol()
                {
                    return protocol;
                }

                public void setName( String name )
                {
                    this.name = name;
                }

                public void setPort( int port )
                {
                    this.port = port;
                }

                public void setProtocol( String protocol )
                {
                    this.protocol = protocol;
                }
            }
        }
    }
}
