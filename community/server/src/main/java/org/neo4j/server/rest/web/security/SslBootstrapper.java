package org.neo4j.server.rest.web.security;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.SignatureException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Date;

import org.bouncycastle.jce.X509Principal;
import org.bouncycastle.x509.X509V3CertificateGenerator;
import org.neo4j.server.configuration.Configurator;

public class SslBootstrapper {

    private Configurator configurator;
    private String hostName;

    public SslBootstrapper(Configurator configurator, String hostName) {
        this.configurator = configurator;
        this.hostName = hostName;
    }
    
    public SslConfiguration init() {
        
        File keyStore = new File(getKeyStorePath());
        String keyStorePassword = getKeyStorePassword();
        String keyPassword = getKeyPassword();
        
        if(!keyStore.exists()) {
            createSelfSignedCertificate(keyStore, keyStorePassword, keyPassword);
        }
        
        return new SslConfiguration(keyStore.getAbsolutePath(), keyStorePassword, keyPassword);
    }
    
    private void createSelfSignedCertificate(File keyStore,
            String keyStorePassword, String keyPassword) {
        try {
            KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
            keyPairGenerator.initialize(1024);  
            KeyPair KPair = keyPairGenerator.generateKeyPair();  
            
            X509V3CertificateGenerator v3CertGen = new X509V3CertificateGenerator();  
            
            v3CertGen.setSerialNumber(BigInteger.valueOf(new SecureRandom().nextInt()));  
            v3CertGen.setIssuerDN(new X509Principal("CN=" + hostName + ", OU=None, O=None L=None, C=None"));  
            v3CertGen.setNotBefore(new Date(System.currentTimeMillis() - 1000L * 60 * 60 * 24 * 30));  
            v3CertGen.setNotAfter(new Date(System.currentTimeMillis() + (1000L * 60 * 60 * 24 * 365*10)));  
            v3CertGen.setSubjectDN(new X509Principal("CN=" + hostName + ", OU=None, O=None L=None, C=None"));
            
            v3CertGen.setPublicKey(KPair.getPublic());  
            v3CertGen.setSignatureAlgorithm("MD5WithRSAEncryption");
            
            X509Certificate PKCertificate = v3CertGen.generateX509Certificate(KPair.getPrivate()); 
            
            FileOutputStream fos = new FileOutputStream("/tmp/testCert.cert");  
            fos.write(PKCertificate.getEncoded());  
            fos.close();
            
            KeyStore privateKS = KeyStore.getInstance("JKS");  
            
            privateKS.setKeyEntry("sample.alias", KPair.getPrivate(),  
                    keyPassword.toCharArray(),  
                    new java.security.cert.Certificate[]{PKCertificate});
            
            privateKS.store( new FileOutputStream(keyStore.getAbsolutePath()), keyStorePassword.toCharArray());
            
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InvalidKeyException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (SecurityException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (SignatureException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (CertificateEncodingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (KeyStoreException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (CertificateException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }  
    }

    private String getKeyStorePath() {
        return configurator.configuration().getString(Configurator.WEBSERVER_SSL_KEYSTORE_PATH_PROPERTY_KEY, Configurator.DEFAULT_WEBSERVER_SSL_KEYSTORE_PATH);
    }
    
    private String getKeyStorePassword() {
        return configurator.configuration().getString(Configurator.WEBSERVER_SSL_KEYSTORE_PASSWORD_PROPERTY_KEY, Configurator.DEFAULT_WEBSERVER_SSL_KEYSTORE_PASSWORD);
    }
    
    private String getKeyPassword() {
        return configurator.configuration().getString(Configurator.WEBSERVER_SSL_KEY_PASSWORD_PROPERTY_KEY, Configurator.DEFAULT_WEBSERVER_SSL_KEY_PASSWORD);
    }
    
}
