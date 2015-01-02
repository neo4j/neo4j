/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.server.security.auth;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.server.security.auth.exception.IllegalTokenException;
import org.neo4j.server.security.auth.exception.IllegalUsernameException;

/**
 * Stores user auth data. In memory, but backed by persistent storage so changes to this repository will survive
 * JVM restarts and crashes.
 */
public class FileUserRepository extends LifecycleAdapter implements UserRepository
{
    private final FileSystemAbstraction fs;
    private final File dbFile;

    /**
     * Used while writing to the dbfile, the whole file is first written to this file, so that we can recover
     * if we crash.
     */
    private final File tempFile;

    /** Quick lookup of users by name */
    private final Map<String, User> usersByName = new ConcurrentHashMap<>();

    /** Master list of users */
    private volatile List<User> users = new ArrayList<>();

    private final UserSerialization serialization = new UserSerialization();

    public FileUserRepository( FileSystemAbstraction fs, File file )
    {
        this.fs = fs;
        this.dbFile = file;
        this.tempFile = new File(file.getAbsolutePath() + ".tmp");
    }

    @Override
    public User get( String name )
    {
        return usersByName.get( name );
    }

    @Override
    public void start() throws Throwable
    {
        if(fs.fileExists( dbFile ))
        {
            loadUsersFromFile(dbFile);
        }
        else if(fs.fileExists( tempFile ))
        {
            fs.renameFile( tempFile, dbFile );
            loadUsersFromFile( dbFile );
            fs.deleteFile( tempFile );
        }
    }

    @Override
    public synchronized void save( User user ) throws IllegalTokenException, IOException, IllegalUsernameException
    {
        // Assert input is ok
        if(user.token() != User.NO_TOKEN && !isValidToken( user.token() ))
        {
            throw new IllegalTokenException( "Invalid token provided, cannot store user." );
        }
        if(!isValidName(user.name()))
        {
            throw new IllegalUsernameException( "'" + user.name() + "' is not a valid user name." );
        }

        // Copy-on-write for the users list
        List<User> newUsers = new ArrayList<>(users);
        boolean replacedExisting = false;
        for ( int i = 0; i < newUsers.size(); i++ )
        {
            User other = newUsers.get( i );
            if( other.name().equals( user.name() ))
            {
                newUsers.set( i, user );
                replacedExisting = true;
            }
            else if ( user.token() != User.NO_TOKEN && other.tokenEquals( user.token() ) )
            {
                throw new IllegalTokenException( "The specified token is already in use." );
            }
        }

        if(!replacedExisting)
        {
            newUsers.add( user );
        }

        users = newUsers;

        commitToDisk();

        usersByName.put( user.name(), user );
    }

    @Override
    public int numberOfUsers()
    {
        return users.size();
    }

    @Override
    public boolean isValidName( String name )
    {
        return name.matches( "^[a-zA-Z0-9_]+$" );
    }

    @Override
    public boolean isValidToken( String token )
    {
        return token.matches( "^[a-fA-F0-9]+$" );
    }

    /* Assumes synchronization elsewhere */
    private void commitToDisk() throws IOException
    {
        writeUsersToFile( tempFile );
        writeUsersToFile( dbFile );
        fs.deleteFile( tempFile );
    }

    private void writeUsersToFile( File fileToWriteTo ) throws IOException
    {
        if(!fs.fileExists( fileToWriteTo.getParentFile() ))
        {
            fs.mkdirs( fileToWriteTo.getParentFile() );
        }
        if(fs.fileExists( fileToWriteTo ))
        {
            fs.deleteFile( fileToWriteTo );
        }
        try(OutputStream out = fs.openAsOutputStream( fileToWriteTo, false ))
        {
            out.write( serialization.serialize( users ) );
            out.flush();
        }
    }

    private void loadUsersFromFile( File fileToLoadFrom ) throws IOException
    {
        if(fs.fileExists( fileToLoadFrom ))
        {
            List<User> loadedUsers;
            try(InputStream in = fs.openAsInputStream( fileToLoadFrom ))
            {
                byte[] bytes = new byte[(int)fs.getFileSize( fileToLoadFrom )];
                int offset = 0;
                while(offset < bytes.length)
                {
                    int read = in.read( bytes, offset, bytes.length - offset );
                    if(read == -1) break;
                    offset += read;
                }
                loadedUsers = serialization.deserializeUsers( bytes );
            }

            if(loadedUsers == null)
            {
                throw new IllegalStateException( "Failed to read authentication file: " + fileToLoadFrom.getAbsolutePath() );
            }

            users = loadedUsers;
            for ( User user : users )
            {
                usersByName.put( user.name(), user );
            }
        }
    }

    @Override
    public Iterator<User> iterator()
    {
        return users.iterator();
    }


}
