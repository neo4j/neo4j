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
package org.neo4j.test.rule.system;

import java.io.FileDescriptor;
import java.net.InetAddress;
import java.security.Permission;

public class TestSecurityManager extends SecurityManager
{

    private SecurityManager securityManager;

    TestSecurityManager( SecurityManager securityManager )
    {
        this.securityManager = securityManager;
    }

    @Override
    public void checkExit( int status )
    {
        throw new SystemExitError( status );
    }

    @Override
    public Object getSecurityContext()
    {
        return managerExists() ? securityManager.getSecurityContext() : super.getSecurityContext();
    }

    @Override
    public void checkPermission( Permission perm )
    {
        // if original security manager exists delegate permission check to it
        // otherwise silently allow everything
        if ( managerExists() )
        {
            securityManager.checkPermission( perm );
        }
    }

    @Override
    public void checkPermission( Permission perm, Object context )
    {
        if ( managerExists() )
        {
            securityManager.checkPermission( perm, context );
        }
        else
        {
            super.checkPermission( perm, context );
        }
    }

    @Override
    public void checkCreateClassLoader()
    {
        if ( managerExists() )
        {
            securityManager.checkCreateClassLoader();
        }
        else
        {
            super.checkCreateClassLoader();
        }
    }

    @Override
    public void checkAccess( Thread t )
    {
        if ( managerExists() )
        {
            securityManager.checkAccess( t );
        }
        else
        {
            super.checkAccess( t );
        }
    }

    @Override
    public void checkAccess( ThreadGroup g )
    {
        if ( managerExists() )
        {
            securityManager.checkAccess( g );
        }
        else
        {
            super.checkAccess( g );
        }
    }

    @Override
    public void checkExec( String cmd )
    {
        if ( managerExists() )
        {
            securityManager.checkExec( cmd );
        }
        else
        {
            super.checkExec( cmd );
        }
    }

    @Override
    public void checkLink( String lib )
    {
        if ( managerExists() )
        {
            securityManager.checkLink( lib );
        }
        else
        {
            super.checkLink( lib );
        }
    }

    @Override
    public void checkRead( FileDescriptor fd )
    {
        if ( managerExists() )
        {
            securityManager.checkRead( fd );
        }
        else
        {
            super.checkRead( fd );
        }
    }

    @Override
    public void checkRead( String file )
    {
        if ( managerExists() )
        {
            securityManager.checkRead( file );
        }
        else
        {
            super.checkRead( file );
        }
    }

    @Override
    public void checkRead( String file, Object context )
    {
        if ( managerExists() )
        {
            securityManager.checkRead( file, context );
        }
        else
        {
            super.checkRead( file, context );
        }
    }

    @Override
    public void checkWrite( FileDescriptor fd )
    {
        if ( managerExists() )
        {
            securityManager.checkWrite( fd );
        }
        else
        {
            super.checkWrite( fd );
        }
    }

    @Override
    public void checkWrite( String file )
    {
        if ( managerExists() )
        {
            securityManager.checkWrite( file );
        }
        else
        {
            super.checkWrite( file );
        }
    }

    @Override
    public void checkDelete( String file )
    {
        if ( managerExists() )
        {
            securityManager.checkDelete( file );
        }
        else
        {
            super.checkDelete( file );
        }
    }

    @Override
    public void checkConnect( String host, int port )
    {
        if ( managerExists() )
        {
            securityManager.checkConnect( host, port );
        }
        else
        {
            super.checkConnect( host, port );
        }
    }

    @Override
    public void checkConnect( String host, int port, Object context )
    {
        if ( managerExists() )
        {
            securityManager.checkConnect( host, port, context );
        }
        else
        {
            super.checkConnect( host, port, context );
        }
    }

    @Override
    public void checkListen( int port )
    {
        if ( managerExists() )
        {
            securityManager.checkListen( port );
        }
        else
        {
            super.checkListen( port );
        }
    }

    @Override
    public void checkAccept( String host, int port )
    {
        if ( managerExists() )
        {
            securityManager.checkAccept( host, port );
        }
        else
        {
            super.checkAccept( host, port );
        }
    }

    @Override
    public void checkMulticast( InetAddress maddr )
    {
        if ( managerExists() )
        {
            securityManager.checkMulticast( maddr );
        }
        else
        {
            super.checkMulticast( maddr );
        }
    }

    @Override
    public void checkMulticast( InetAddress maddr, byte ttl )
    {
        if ( managerExists() )
        {
            securityManager.checkMulticast( maddr, ttl );
        }
        else
        {
            super.checkMulticast( maddr, ttl );
        }
    }

    @Override
    public void checkPropertiesAccess()
    {
        if ( managerExists() )
        {
            securityManager.checkPropertiesAccess();
        }
        else
        {
            super.checkPropertiesAccess();
        }
    }

    @Override
    public void checkPropertyAccess( String key )
    {
        if ( managerExists() )
        {
            securityManager.checkPropertyAccess( key );
        }
        else
        {
            super.checkPropertyAccess( key );
        }
    }

    @Override
    public boolean checkTopLevelWindow( Object window )
    {
        return managerExists() ? securityManager.checkTopLevelWindow( window ) : super.checkTopLevelWindow( window );
    }

    @Override
    public void checkPrintJobAccess()
    {
        if ( managerExists() )
        {
            securityManager.checkPrintJobAccess();
        }
        else
        {
            super.checkPrintJobAccess();
        }
    }

    @Override
    public void checkSystemClipboardAccess()
    {
        if ( managerExists() )
        {
            securityManager.checkSystemClipboardAccess();
        }
        else
        {
            super.checkSystemClipboardAccess();
        }
    }

    @Override
    public void checkAwtEventQueueAccess()
    {
        if ( managerExists() )
        {
            securityManager.checkAwtEventQueueAccess();
        }
        else
        {
            super.checkAwtEventQueueAccess();
        }
    }

    @Override
    public void checkPackageAccess( String pkg )
    {
        if ( managerExists() )
        {
            securityManager.checkPackageAccess( pkg );
        }
        else
        {
            super.checkPackageAccess( pkg );
        }
    }

    @Override
    public void checkPackageDefinition( String pkg )
    {
        if ( managerExists() )
        {
            securityManager.checkPackageDefinition( pkg );
        }
        else
        {
            super.checkPackageDefinition( pkg );
        }
    }

    @Override
    public void checkSetFactory()
    {
        if ( managerExists() )
        {
            securityManager.checkSetFactory();
        }
        else
        {
            super.checkSetFactory();
        }
    }

    @Override
    public void checkMemberAccess( Class<?> clazz, int which )
    {
        if ( managerExists() )
        {
            securityManager.checkMemberAccess( clazz, which );
        }
        else
        {
            super.checkMemberAccess( clazz, which );
        }
    }

    @Override
    public void checkSecurityAccess( String target )
    {
        if ( managerExists() )
        {
            securityManager.checkSecurityAccess( target );
        }
        else
        {
            super.checkSecurityAccess( target );
        }
    }

    @Override
    public ThreadGroup getThreadGroup()
    {
        return managerExists() ? securityManager.getThreadGroup() : super.getThreadGroup();
    }

    private boolean managerExists()
    {
        return securityManager != null;
    }

}
