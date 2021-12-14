package org.neo4j.kernel.database;

import java.util.Comparator;
import java.util.Objects;

import org.neo4j.configuration.helpers.RemoteUri;

/**
 * Implementations of this interface represent different kinds of Database reference.
 *
 * - {@link Internal} references point to databases which are present in this DBMS.
 * - {@link External} references point to databases which are not present in this DBMS.
 *
 * A database may have multiple references, each with a different alias.
 * The reference whose {@link #alias()} corresponds to the database's original name is known as the primary reference.
 */
public abstract class DatabaseReference implements Comparable<DatabaseReference>
{
    private static final Comparator<DatabaseReference> referenceComparator = Comparator.comparing( a -> a.alias().name(), String::compareToIgnoreCase );
    private static final Comparator<DatabaseReference> nullSafeReferenceComparator = Comparator.nullsLast( referenceComparator );

    /**
     * @return the alias associated with this database reference
     */
    public abstract NormalizedDatabaseName alias();

    /**
     * @return whether the alias associated with this reference is the database's original/true name
     */
    public abstract boolean isPrimary();

    /**
     * @return whether the reference refers to a database which is present on this physical instance
     */
    public abstract boolean isRemote();


    @Override
    public int compareTo( DatabaseReference that )
    {
        return nullSafeReferenceComparator.compare( this, that );
    }

    /**
     * External references point to databases which are not stored within this DBMS.
     */
    public static final class External extends DatabaseReference
    {
        private final NormalizedDatabaseName targetName;
        private final NormalizedDatabaseName name;
        private final RemoteUri remoteUri;

        public External( NormalizedDatabaseName targetName, NormalizedDatabaseName name, RemoteUri remoteUri )
        {
            this.targetName = targetName;
            this.name = name;
            this.remoteUri = remoteUri;
        }

        @Override
        public NormalizedDatabaseName alias()
        {
            return name;
        }

        @Override
        public boolean isPrimary()
        {
            return false;
        }

        @Override
        public boolean isRemote()
        {
            return true;
        }

        public RemoteUri remoteUri()
        {
            return remoteUri;
        }

        public NormalizedDatabaseName remoteName()
        {
            return targetName;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            External remote = (External) o;
            return Objects.equals( targetName, remote.targetName ) && Objects.equals( name, remote.name ) &&
                   Objects.equals( remoteUri, remote.remoteUri );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( targetName, name, remoteUri );
        }

        @Override
        public String toString()
        {
            return "DatabaseReference.External{" +
                   "remoteName=" + targetName +
                   ", name=" + name +
                   ", remoteUri=" + remoteUri +
                   '}';
        }
    }

    /**
     * Local references point to databases which are stored within this DBMS.
     *
     * Note, however, that a local reference may point to databases not stored on this physical instance.
     * Whether a reference points to a database present on this instance is represented by {@link #isRemote()}.
     */
    public static final class Internal extends DatabaseReference
    {
        private final NormalizedDatabaseName name;
        private final NamedDatabaseId namedDatabaseId;

        public Internal( NormalizedDatabaseName name, NamedDatabaseId namedDatabaseId )
        {
            this.name = name;
            this.namedDatabaseId = namedDatabaseId;
        }

        public NamedDatabaseId databaseId()
        {
            return namedDatabaseId;
        }

        @Override
        public NormalizedDatabaseName alias()
        {
            return name;
        }

        @Override
        public boolean isPrimary()
        {
            return Objects.equals( name.name(), namedDatabaseId.name() );
        }

        @Override
        public boolean isRemote()
        {
            return false;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            Internal internal = (Internal) o;
            return Objects.equals( name, internal.name ) && Objects.equals( namedDatabaseId, internal.namedDatabaseId );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( name, namedDatabaseId );
        }

        @Override
        public String toString()
        {
            return "DatabaseReference.Internal{" +
                   "name=" + name +
                   ", namedDatabaseId=" + namedDatabaseId +
                   '}';
        }
    }


}


