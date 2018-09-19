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

/**
 * See <a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.11/#status-v1-meta">Status</a>
 */
public class Status extends KubernetesType
{
    private String status;
    private String message;
    private String reason;
    private int code;

    public String status()
    {
        return status;
    }

    public void setStatus( String status )
    {
        this.status = status;
    }

    public String message()
    {
        return message;
    }

    public void setMessage( String message )
    {
        this.message = message;
    }

    public String reason()
    {
        return reason;
    }

    public void setReason( String reason )
    {
        this.reason = reason;
    }

    public int code()
    {
        return code;
    }

    public void setCode( int code )
    {
        this.code = code;
    }

    @Override
    public <T> T handle( Visitor<T> visitor )
    {
        return visitor.visit( this );
    }

    @Override
    public String toString()
    {
        return "Status{" + "status='" + status + '\'' + ", message='" + message + '\'' + ", reason='" + reason + '\'' + ", code=" + code + '}';
    }
}
