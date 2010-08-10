package slavetest;

public interface Fetcher<T>
{
    T fetch();
    
    void close();
}
