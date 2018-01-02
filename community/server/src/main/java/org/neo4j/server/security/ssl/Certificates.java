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
package org.neo4j.server.security.ssl;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;
import org.bouncycastle.util.io.pem.PemWriter;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import javax.crypto.NoSuchPaddingException;

public class Certificates
{
    /** Generating SSL certificates takes a long time. This non-official setting allows us to use a fast source of randomness when running tests */
    private static final boolean useInsecureCertificateGeneration = Boolean.getBoolean( "org.neo4j.useInsecureCertificateGeneration" );
    private static final String CERTIFICATE_TYPE = "X.509";
    private static final String DEFAULT_ENCRYPTION = "RSA";
    private final SecureRandom random;
    /** Current time minus 1 year, just in case software clock goes back due to time synchronization */
    private static final Date NOT_BEFORE = new Date( System.currentTimeMillis() - 86400000L * 365 );
    /** The maximum possible value in X.509 specification: 9999-12-31 23:59:59 */
    private static final Date NOT_AFTER = new Date( 253402300799000L );
    private static final Provider PROVIDER = new BouncyCastleProvider();

    static {
        Security.addProvider( PROVIDER );
    }

    public Certificates()
    {
        random = useInsecureCertificateGeneration ? new InsecureRandom() : new SecureRandom();
    }

    public void createSelfSignedCertificate(File certificatePath, File privateKeyPath, String hostName)
            throws GeneralSecurityException, IOException, OperatorCreationException
    {
        KeyPairGenerator keyGen = KeyPairGenerator.getInstance( DEFAULT_ENCRYPTION );
        keyGen.initialize( 1024, random );
        KeyPair keypair = keyGen.generateKeyPair();

        // Prepare the information required for generating an X.509 certificate.
        X500Name owner = new X500Name( "CN=" + hostName );
        X509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(
                owner, new BigInteger( 64, random ), NOT_BEFORE, NOT_AFTER, owner, keypair.getPublic() );

        PrivateKey privateKey = keypair.getPrivate();
        ContentSigner signer = new JcaContentSignerBuilder( "SHA512WithRSAEncryption" ).build( privateKey );
        X509CertificateHolder certHolder = builder.build( signer );
        X509Certificate cert = new JcaX509CertificateConverter().setProvider( PROVIDER ).getCertificate( certHolder );

        //check so that cert is valid
        cert.verify( keypair.getPublic() );

        //write to disk
        writePem( "CERTIFICATE", cert.getEncoded(), certificatePath );
        writePem( "PRIVATE KEY", privateKey.getEncoded(), privateKeyPath );
    }

    public Certificate[] loadCertificates(File certFile) throws CertificateException, IOException
    {
        CertificateFactory certFactory = CertificateFactory.getInstance( CERTIFICATE_TYPE );
        Collection<Certificate> certificates = new LinkedList<>();

        try(PemReader r = new PemReader( new FileReader( certFile ) ))
        {
            for( PemObject pemObject = r.readPemObject(); pemObject != null; pemObject = r.readPemObject() )
            {
                byte[] encodedCert = pemObject.getContent();
                certificates.addAll( certFactory.generateCertificates( new ByteArrayInputStream( encodedCert ) ) );
            }
        }

        if(certificates.size() == 0)
        {
            // Ok, failed to read as PEM file, try and read it as raw binary certificate
            try ( FileInputStream in = new FileInputStream( certFile ) )
            {
                certificates = (Collection<Certificate>)certFactory.generateCertificates( in );
            }
        }

        return certificates.toArray( new Certificate[certificates.size()] );
    }

    public PrivateKey loadPrivateKey(File privateKeyFile)
            throws IOException, NoSuchAlgorithmException,
            InvalidKeySpecException, NoSuchPaddingException,
            InvalidKeyException, InvalidAlgorithmParameterException
    {
        try(PemReader r = new PemReader( new FileReader( privateKeyFile ) ))
        {
            PemObject pemObject = r.readPemObject();
            if( pemObject != null )
            {
                byte[] encodedKey = pemObject.getContent();
                KeySpec keySpec = new PKCS8EncodedKeySpec( encodedKey );
                try
                {
                    return KeyFactory.getInstance( "RSA" ).generatePrivate( keySpec );
                }
                catch ( InvalidKeySpecException ignore )
                {
                    try
                    {
                        return KeyFactory.getInstance( "DSA" ).generatePrivate( keySpec );
                    }
                    catch ( InvalidKeySpecException ignore2 )
                    {
                        try
                        {
                            return KeyFactory.getInstance( "EC" ).generatePrivate( keySpec );
                        }
                        catch ( InvalidKeySpecException e )
                        {
                            throw new InvalidKeySpecException( "Neither RSA, DSA nor EC worked", e );
                        }
                    }
                }
            }
        }

        // Ok, failed to read as PEM file, try and read it as a raw binary private key
        try(DataInputStream in = new DataInputStream(new FileInputStream(privateKeyFile)))
        {
            byte[] keyBytes = new byte[(int) privateKeyFile.length()];
            in.readFully( keyBytes );

            KeySpec keySpec = new PKCS8EncodedKeySpec(keyBytes);

            return KeyFactory.getInstance( DEFAULT_ENCRYPTION ).generatePrivate(keySpec);
        }
    }

    private void writePem( String type, byte[] encodedContent, File path ) throws IOException
    {
        path.getParentFile().mkdirs();
        try ( PemWriter writer = new PemWriter( new FileWriter( path ) ) )
        {
            writer.writeObject( new PemObject( type, encodedContent ) );
            writer.flush();
        }
        path.setReadable( false, false );
        path.setWritable( false, false );
        path.setReadable( true );
        path.setWritable( true );
    }
}
