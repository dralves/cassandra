/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.tracing;

import java.nio.ByteBuffer;

import com.google.common.base.Throwables;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

/**
 * Thrift type to be able to serialize and deserialize thrift objects.
 */
public class ThriftType extends AbstractType<TBase<?, ?>>
{

    @SuppressWarnings("rawtypes")
    public static ThriftType getInstance(Class<? extends TBase> thriftObjectClass)
    {
        return new ThriftType(thriftObjectClass);
    }

    private TSerializer serializer;
    private TDeserializer deserializer;
    @SuppressWarnings("rawtypes")
    private Class<? extends TBase> objectClass;

    @SuppressWarnings("rawtypes")
    ThriftType(Class<? extends TBase> objectClass)
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
    public String toString()
    {
        return getClass().getName() + "(" + objectClass.getName() + ")";
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
