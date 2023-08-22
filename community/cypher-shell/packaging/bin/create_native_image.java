/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Scanner;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * This is Java classed to be executed as a script {@code java create_native_image} (as JEP 330 single source file). It will take the "no-arch" distribution of
 * cypher-shell created via standard {@code mvn package -pl :cypher-shell} on the root Neo4j project, figure out the current architecture the same way as the
 * os-maven-plugin and eventually call GraalVMs {@code native-image} tool.
 * <p>
 * The script is supposed to run in {@code ghcr.io/graalvm/graalvm-ce:21.3.0} with Java 17.
 * <p>
 * The reason this is done as Java code is simple: While the GraalVM images are available for broad range of architectures (see
 * <a href="https://www.graalvm.org/docs/getting-started/container-images/">container-images</a>, the images itself differ vastly.
 * What is guaranteed is that {@code java}  and {@code gu} are available to add additional Graal tooling (like the {@literal native-image}). We can just use
 * this uniformly on all architectures to create the native image.
 *
 * <p>
 * Contains code from
 * <a href="https://github.com/trustin/os-maven-plugin/blob/os-maven-plugin-1.7.0/src/main/java/kr/motd/maven/os/Detector.java">os-maven-plugin</a>
 * by Trustin Heuiseung Lee, licensed under  Apache License, Version 2.0.
 */
public class create_native_image
{
    private static final String UNKNOWN = "unknown";
    private static final String VERSION_PATTERN = "(?i)" + Pattern.quote( "cypher-shell-" ) + "(\\d+.\\d+.\\d+(-SNAPSHOT)?)";

    public static void main( String... args ) throws Exception
    {
        if ( args.length != 1 )
        {
            throw new IllegalArgumentException( "Usage: java " + create_native_image.class.getName() + " <cypher-shell-X.Y.Z.zip>" );
        }

        var zipFile = Path.of( args[0] ).toAbsolutePath().normalize();
        if ( !Files.exists( zipFile ) )
        {
            throw new IllegalArgumentException( "Zip file " + zipFile + " does not exists" );
        }

        var os = createOsClassifier();

        // Check if a native executable has been prepared via Maven already
        var zipFileName = zipFile.getName( zipFile.getNameCount() - 1 ).toString();
        zipFileName = zipFileName.substring( 0, zipFileName.lastIndexOf( "." ) );

        var existingNativeZip = zipFile.getParent().resolve( zipFileName + "-" + os + ".zip" );
        if (Files.exists( existingNativeZip ) )
        {
            var cypherShell = extractExisting( os, existingNativeZip );
            if ( cypherShell != null && Files.exists( cypherShell ) )
            {
                System.out.println( "Extracted existing native binary to " + cypherShell.toAbsolutePath().normalize() );
                System.exit( 0 );
            }
        }

        var libDir = extractLibs( zipFile ).toAbsolutePath().normalize();
        var target = zipFile.getParent();

        try
        {
            tail( new ProcessBuilder( "native-image", "--version" ).start() );
        }
        catch ( IOException e )
        {
            if ( e.getMessage().contains( "No such file or directory" ) )
            {
                System.out.println( "Installing native-image first" );
                var gu = new ProcessBuilder( "gu", "install", "native-image" ).start();
                tail( gu );
            }
        }

        var executableName = "cypher-shell-";
        var m = Pattern.compile( VERSION_PATTERN ).matcher( zipFileName );
        if(m.find()) {
            executableName += m.group(1) + "-";
        }
        executableName += os;
        var nativeImage = new ProcessBuilder(
                "native-image",
                "-cp", libDir + "/*",
                "--no-fallback",
                "--allow-incomplete-classpath",
                "-H:Class=org.neo4j.shell.Main",
                "-H:Name=" + target + "/" + executableName
        ).redirectErrorStream( true ).start();
        tail( nativeImage );
    }

    static void tail( Process process )
    {
        var s = new Scanner( process.getInputStream() );
        while ( process.isAlive() && s.hasNextLine() )
        {
            System.out.println( s.nextLine() );
        }
    }

    static Path extractExisting( String os, Path zipFile ) throws IOException
    {
        Path cypherShell = null;
        try ( var zipInputStream = new ZipInputStream( new BufferedInputStream( new FileInputStream( zipFile.toFile() ) ) ) )
        {
            ZipEntry currentEntry;
            while ( (currentEntry = zipInputStream.getNextEntry()) != null && cypherShell == null )
            {
                var name = currentEntry.getName();
                var m = Pattern.compile( VERSION_PATTERN + Pattern.quote( "-" + os + "/bin/cypher-shell" ) ).matcher( name );
                if ( !currentEntry.isDirectory() && m.find() )
                {
                    cypherShell = zipFile.getParent().resolve( name.substring( name.lastIndexOf( "/" ) + 1 ) + "-" + m.group(1) + "-" + os );
                    Files.copy( zipInputStream, cypherShell, StandardCopyOption.REPLACE_EXISTING );
                    var attributes = Files.getFileAttributeView( cypherShell, PosixFileAttributeView.class );
                    var permissions = attributes.readAttributes().permissions();
                    permissions.addAll( EnumSet.of( PosixFilePermission.OWNER_EXECUTE, PosixFilePermission.GROUP_EXECUTE, PosixFilePermission.OTHERS_EXECUTE ) );
                    attributes.setPermissions( permissions );
                }
                zipInputStream.closeEntry();
            }
        }
        return cypherShell;
    }

    static Path extractLibs( Path zipFile ) throws IOException
    {
        var libDir = Files.createTempDirectory( "cypher-shell-libs-" );
        try ( var zipInputStream = new ZipInputStream( new BufferedInputStream( new FileInputStream( zipFile.toFile() ) ) ) )
        {
            ZipEntry currentEntry;
            while ( (currentEntry = zipInputStream.getNextEntry()) != null )
            {
                var name = currentEntry.getName();
                if ( !currentEntry.isDirectory() && name.toLowerCase( Locale.ROOT ).contains( "lib/" ) )
                {
                    var jarName = name.substring( name.lastIndexOf( "/" ) + 1 );
                    var jar = libDir.resolve( jarName );
                    Files.copy( zipInputStream, jar );
                }
                zipInputStream.closeEntry();
            }
        }
        return libDir;
    }

    static String createOsClassifier()
    {
        var os = normalizeOs( System.getProperty( "os.name" ) );
        var arch = normalizeArch( System.getProperty( "os.arch" ) );

        return os + "-" + arch;
    }

    private static String normalizeOs( String value )
    {
        value = normalize( value );
        if ( value.startsWith( "aix" ) )
        {
            return "aix";
        }
        if ( value.startsWith( "hpux" ) )
        {
            return "hpux";
        }
        if ( value.startsWith( "os400" ) )
        {
            // Avoid the names such as os4000
            if ( value.length() <= 5 || !Character.isDigit( value.charAt( 5 ) ) )
            {
                return "os400";
            }
        }
        if ( value.startsWith( "linux" ) )
        {
            return "linux";
        }
        if ( value.startsWith( "macosx" ) || value.startsWith( "osx" ) )
        {
            return "osx";
        }
        if ( value.startsWith( "freebsd" ) )
        {
            return "freebsd";
        }
        if ( value.startsWith( "openbsd" ) )
        {
            return "openbsd";
        }
        if ( value.startsWith( "netbsd" ) )
        {
            return "netbsd";
        }
        if ( value.startsWith( "solaris" ) || value.startsWith( "sunos" ) )
        {
            return "sunos";
        }
        if ( value.startsWith( "windows" ) )
        {
            return "windows";
        }
        if ( value.startsWith( "zos" ) )
        {
            return "zos";
        }

        return UNKNOWN;
    }

    private static String normalizeArch( String value )
    {
        value = normalize( value );
        if ( value.matches( "^(x8664|amd64|ia32e|em64t|x64)$" ) )
        {
            return "x86_64";
        }
        if ( value.matches( "^(x8632|x86|i[3-6]86|ia32|x32)$" ) )
        {
            return "x86_32";
        }
        if ( value.matches( "^(ia64w?|itanium64)$" ) )
        {
            return "itanium_64";
        }
        if ( "ia64n".equals( value ) )
        {
            return "itanium_32";
        }
        if ( value.matches( "^(sparc|sparc32)$" ) )
        {
            return "sparc_32";
        }
        if ( value.matches( "^(sparcv9|sparc64)$" ) )
        {
            return "sparc_64";
        }
        if ( value.matches( "^(arm|arm32)$" ) )
        {
            return "arm_32";
        }
        if ( "aarch64".equals( value ) )
        {
            return "aarch_64";
        }
        if ( value.matches( "^(mips|mips32)$" ) )
        {
            return "mips_32";
        }
        if ( value.matches( "^(mipsel|mips32el)$" ) )
        {
            return "mipsel_32";
        }
        if ( "mips64".equals( value ) )
        {
            return "mips_64";
        }
        if ( "mips64el".equals( value ) )
        {
            return "mipsel_64";
        }
        if ( value.matches( "^(ppc|ppc32)$" ) )
        {
            return "ppc_32";
        }
        if ( value.matches( "^(ppcle|ppc32le)$" ) )
        {
            return "ppcle_32";
        }
        if ( "ppc64".equals( value ) )
        {
            return "ppc_64";
        }
        if ( "ppc64le".equals( value ) )
        {
            return "ppcle_64";
        }
        if ( "s390".equals( value ) )
        {
            return "s390_32";
        }
        if ( "s390x".equals( value ) )
        {
            return "s390_64";
        }
        if ( "riscv".equals( value ) )
        {
            return "riscv";
        }
        if ( "e2k".equals( value ) )
        {
            return "e2k";
        }
        return UNKNOWN;
    }

    private static String normalize( String value )
    {
        if ( value == null )
        {
            return "";
        }
        return value.toLowerCase( Locale.ROOT ).replaceAll( "[^a-z0-9]+", "" );
    }
}
