package org.apache.cassandra.tracing;

import java.nio.ByteBuffer;

import com.google.common.base.Throwables;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

public class ThriftObjectType extends AbstractType<TBase<?, ?>>
{

    @SuppressWarnings("rawtypes")
    public static ThriftObjectType getInstance(Class<? extends TBase> thriftObjectClass)
    {
        return new ThriftObjectType(thriftObjectClass);
    }

    private TSerializer serializer;
    private TDeserializer deserializer;
    @SuppressWarnings("rawtypes")
    private Class<? extends TBase> objectClass;

    @SuppressWarnings("rawtypes")
    ThriftObjectType(Class<? extends TBase> objectClass)
    {
        this.objectClass = objectClass;
        this.serializer = new TSerializer();
        this.deserializer = new TDeserializer();
    }

    @Override
    public TBase<?, ?> compose(ByteBuffer bytes)
    {
        TBase<?, ?> obj;
        try
        {
            obj = this.objectClass.newInstance();
            this.deserializer.deserialize(obj, bytes.array());
            return obj;
        }
        catch (Exception e)
        {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public ByteBuffer decompose(TBase<?, ?> value)
    {
        try
        {
            return ByteBuffer.wrap(this.serializer.serialize(value));
        }
        catch (TException e)
        {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public int compare(ByteBuffer arg0, ByteBuffer arg1)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getString(ByteBuffer bytes)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer fromString(String source) throws MarshalException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void validate(ByteBuffer bytes) throws MarshalException
    {
        throw new UnsupportedOperationException();

    }

}
