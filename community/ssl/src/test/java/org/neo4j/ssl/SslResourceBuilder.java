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
package org.neo4j.ssl;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;

import static org.neo4j.ssl.SslResourceBuilder.SignedBy.CA;
import static org.neo4j.ssl.SslResourceBuilder.SignedBy.SELF;

/**
 * This builder has a finite set of pre-generated resource
 * keys and certificates which can be utilized in tests.
 */
public class SslResourceBuilder
{
    private static final String CA_CERTIFICATE_NAME = "cluster.crt";

    private static final String PRIVATE_KEY_NAME = "private.key";
    private static final String PUBLIC_CERT_NAME = "public.crt";

    private static final String SELF_SIGNED_NAME = "selfsigned.crt";
    private static final String REVOKED_NAME = "revoked.crl";
    private static final String CA_SIGNED_NAME = "casigned.crt";

    private static final String TRUSTED_DIR_NAME = "trusted";
    private static final String REVOKED_DIR_NAME = "revoked";

    private static final String CA_BASE_PATH = "test-certificates/ca/";
    private static final String SERVERS_BASE_PATH = "test-certificates/servers/";

    private final int keyId;

    enum SignedBy
    {
        SELF( SELF_SIGNED_NAME ),
        CA( CA_SIGNED_NAME );

        private final String resourceName;

        SignedBy( String resourceName )
        {
            this.resourceName = resourceName;
        }

        public URL keyId( int keyId )
        {
            return resource( resourceName, keyId );
        }
    }

    private final SignedBy signedBy;

    private boolean trustSignedByCA;
    private Set<Integer> trusted = new HashSet<>();
    private Set<Integer> revoked = new HashSet<>();

    private FileSystemAbstraction fsa = new DefaultFileSystemAbstraction();

    private SslResourceBuilder( int keyId, SignedBy signedBy )
    {
        this.keyId = keyId;
        this.signedBy = signedBy;
    }

    public static SslResourceBuilder selfSignedKeyId( int keyId )
    {
        return new SslResourceBuilder( keyId, SELF );
    }

    public static SslResourceBuilder caSignedKeyId( int keyId )
    {
        return new SslResourceBuilder( keyId, CA );
    }

    public SslResourceBuilder trustKeyId( int keyId )
    {
        trusted.add( keyId );
        return this;
    }

    public SslResourceBuilder trustSignedByCA()
    {
        this.trustSignedByCA = true;
        return this;
    }

    public SslResourceBuilder revoke( int keyId )
    {
        revoked.add( keyId );
        return this;
    }

    public SslResource install( File targetDirectory ) throws IOException
    {
        return install( targetDirectory, CA_CERTIFICATE_NAME );
    }

    public SslResource install( File targetDirectory, String trustedFileName ) throws IOException
    {
        File targetKey = new File( targetDirectory, PRIVATE_KEY_NAME );
        File targetCertificate = new File( targetDirectory, PUBLIC_CERT_NAME );
        File targetTrusted = new File( targetDirectory, TRUSTED_DIR_NAME );
        File targetRevoked = new File( targetDirectory, REVOKED_DIR_NAME );

        fsa.mkdir( targetTrusted );
        fsa.mkdir( targetRevoked );

        for ( int trustedKeyId : trusted )
        {
            File targetTrustedCertificate = new File( targetTrusted, String.valueOf( trustedKeyId ) + ".crt" );
            copy( resource( SELF_SIGNED_NAME, trustedKeyId ), targetTrustedCertificate );
        }

        for ( int revokedKeyId : revoked )
        {
            File targetRevokedCRL = new File( targetRevoked, String.valueOf( revokedKeyId ) + ".crl" );
            copy( resource( REVOKED_NAME, revokedKeyId ), targetRevokedCRL );
        }

        if ( trustSignedByCA )
        {
            File targetTrustedCertificate = new File( targetTrusted, trustedFileName );
            copy( resource( trustedFileName ), targetTrustedCertificate );
        }

        copy( resource( PRIVATE_KEY_NAME, keyId ), targetKey );
        copy( signedBy.keyId( keyId ), targetCertificate );

        return new SslResource( targetKey, targetCertificate, targetTrusted, targetRevoked );
    }

    private static URL resource( String filename, int keyId )
    {
        return SslResourceBuilder.class.getResource( SERVERS_BASE_PATH + String.valueOf( keyId ) + "/" + filename );
    }

    private URL resource( String filename )
    {
        return SslResourceBuilder.class.getResource( CA_BASE_PATH + filename );
    }

    private void copy( URL in, File outFile ) throws IOException
    {
        try ( InputStream is = in.openStream();
              OutputStream os = fsa.openAsOutputStream( outFile, false ) )
        {
            while ( is.available() > 0 )
            {
                byte[] buf = new byte[8192];
                int nBytes = is.read( buf );
                os.write( buf, 0, nBytes );
            }
        }
    }
}
