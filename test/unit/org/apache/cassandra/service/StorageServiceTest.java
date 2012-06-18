/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.apache.cassandra.service;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.net.InetAddresses;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.dht.BigIntegerToken;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.locator.TokenMetadata;

public class StorageServiceTest
{
    private static final String DC1 = "DC1";
    private static final String DC2 = "DC2";
    private static final String DC3 = "DC3";
    private static final String RAC1 = "RAC1";
    private static final String RAC2 = "RAC2";

    private static InetAddress ringPos0 = InetAddresses.forString("127.0.0.1");
    private static InetAddress ringPos1 = InetAddresses.forString("127.0.0.2");
    private static InetAddress ringPos2 = InetAddresses.forString("127.0.0.3");
    private static InetAddress ringPos3 = InetAddresses.forString("127.0.0.4");
    private static InetAddress ringPos4 = InetAddresses.forString("127.0.0.5");
    private static InetAddress ringPos5 = InetAddresses.forString("127.0.0.6");
    private static InetAddress ringPos6 = InetAddresses.forString("127.0.0.7");

    private static Token token0 = new BigIntegerToken("0");
    private static Token token0PlusOne = new BigIntegerToken("1");
    private static Token token0PlusTwo = new BigIntegerToken("2");
    private static Token tokenOneQuarter = new BigIntegerToken("42535295865117307932921825928971026432");
    private static Token tokenHalf = new BigIntegerToken("85070591730234615865843651857942052864");
    private static Token tokenHalfPlusOne = new BigIntegerToken("85070591730234615865843651857942052865");
    private static Token tokenThreeQuarters = new BigIntegerToken("127605887595351923798765477786913079296");

    private static Map<InetAddress, String> addressToDcMap = new ImmutableMap.Builder<InetAddress, String>()
            .put(ringPos0, DC1).put(ringPos1, DC2).put(ringPos2, DC2).put(ringPos3, DC3).put(ringPos4, DC3)
            .put(ringPos5, DC3).put(ringPos6, DC3).build();

    private static Map<InetAddress, String> addressToRackMap = new ImmutableMap.Builder<InetAddress, String>()
            .put(ringPos0, RAC1).put(ringPos1, RAC1).put(ringPos2, RAC2).put(ringPos3, RAC1).put(ringPos4, RAC1)
            .put(ringPos5, RAC2).put(ringPos6, RAC2).build();

    private static BiMap<Token, InetAddress> sameDcRing = new ImmutableBiMap.Builder<Token, InetAddress>()
            .put(token0, ringPos0).put(tokenOneQuarter, ringPos1).put(tokenHalf, ringPos2)
            .put(tokenThreeQuarters, ringPos3).build();

    private static BiMap<Token, InetAddress> multiDcRing = new ImmutableBiMap.Builder<Token, InetAddress>()
            .put(token0, ringPos0).put(token0PlusOne, ringPos1).put(tokenHalf, ringPos2).put(token0PlusTwo, ringPos3)
            .put(tokenOneQuarter, ringPos4).put(tokenHalfPlusOne, ringPos5).put(tokenThreeQuarters, ringPos6).build();

    @BeforeClass
    public static void setUp() throws IOException
    {
        DatabaseDescriptor.setEndpointSnitch(new IEndpointSnitch()
        {
            @Override
            public void sortByProximity(InetAddress address, List<InetAddress> addresses)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public List<InetAddress> getSortedListByProximity(InetAddress address,
                    Collection<InetAddress> unsortedAddress)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public String getRack(InetAddress endpoint)
            {
                return addressToRackMap.get(endpoint);
            }

            @Override
            public String getDatacenter(InetAddress endpoint)
            {
                return addressToDcMap.get(endpoint);
            }

            @Override
            public int compareEndpoints(InetAddress target, InetAddress a1, InetAddress a2)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void gossiperStarting()
            {
            }
        });
        SchemaLoader.loadSchema(false);
    }

    @AfterClass
    public static void tearDown()
    {
        SchemaLoader.stopGossiper();
    }

    @Test
    public void testEffectiveOwnershipWithSimpleStrategy() throws ConfigurationException, IOException
    {

        TokenMetadata metadata = new TokenMetadata(sameDcRing);
        StorageService.instance.setTokenMetadataUnsafe(metadata);

        Table.open("Keyspace1").createReplicationStrategy(
                KSMetaData.newKeyspace("Keyspace1", SimpleStrategy.class.getName(),
                        ImmutableMap.of("replication_factor", "2")));

        StorageService.instance.setPartitionerUnsafe(new RandomPartitioner());
        Map<String, Float> ownership = StorageService.instance.effectiveOwnership("Keyspace1");

        assertEquals(ownership.size(), 4);
        for (Map.Entry<String, Float> entry : ownership.entrySet())
        {
            assertEquals("unexpected ring ownership", 0.5f, entry.getValue().floatValue(), 0);
        }
    }

    @Test
    public void testEffectiveOwnershipWithNetworkTopologyStrategy() throws ConfigurationException, IOException
    {

        TokenMetadata metadata = new TokenMetadata(multiDcRing);
        StorageService.instance.setTokenMetadataUnsafe(metadata);

        Table.open("Keyspace1").createReplicationStrategy(
                KSMetaData.newKeyspace("Keyspace1", NetworkTopologyStrategy.class.getName(),
                        ImmutableMap.of(DC1, "1", DC2, "1", DC3, "3")));

        StorageService.instance.setPartitionerUnsafe(new RandomPartitioner());
        Map<String, Float> ownership = StorageService.instance.effectiveOwnership("Keyspace1");

        assertEquals(7, ownership.size());

        assertEquals("unexpected token position",
                Iterables.get(ownership.keySet(), 0), token0.toString());
        assertEquals("unexpected token position",
                Iterables.get(ownership.keySet(), 1), token0PlusOne.toString());
        assertEquals("unexpected token position",
                Iterables.get(ownership.keySet(), 2), tokenHalf.toString());
        assertEquals("unexpected token position",
                Iterables.get(ownership.keySet(), 3), token0PlusTwo.toString());
        assertEquals("unexpected token position",
                Iterables.get(ownership.keySet(), 4), tokenOneQuarter.toString());
        assertEquals("unexpected token position",
                Iterables.get(ownership.keySet(), 5), tokenHalfPlusOne.toString());
        assertEquals("unexpected token position",
                Iterables.get(ownership.keySet(), 6), tokenThreeQuarters.toString());

        assertEquals("unexpected ring ownership",
                Iterables.get(ownership.values(), 0), 1f, 0);
        assertEquals("unexpected ring ownership",
                Iterables.get(ownership.values(), 1), 0.5f, 0);
        assertEquals("unexpected ring ownership",
                Iterables.get(ownership.values(), 2), 0.5f, 0);
        assertEquals("unexpected ring ownership",
                Iterables.get(ownership.values(), 3), 0.75f, 0);
        assertEquals("unexpected ring ownership",
                Iterables.get(ownership.values(), 4), 0.75f, 0);
        assertEquals("unexpected ring ownership",
                Iterables.get(ownership.values(), 5), 0.75f, 0);
        assertEquals("unexpected ring ownership",
                Iterables.get(ownership.values(), 6), 0.75f, 0);
    }
}
