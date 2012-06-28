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
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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
import org.apache.cassandra.utils.Pair;

public class StorageServiceTest
{
    private static final String DC1 = "DC1";
    private static final String DC2 = "DC2";
    private static final String DC3 = "DC3";
    private static final String RAC1 = "RAC1";
    private static final String RAC2 = "RAC2";

    private static InetAddress ep0 = InetAddresses.forString("127.0.0.1");
    private static InetAddress ep1 = InetAddresses.forString("127.0.0.2");
    private static InetAddress ep2 = InetAddresses.forString("127.0.0.3");
    private static InetAddress ep3 = InetAddresses.forString("127.0.0.4");
    private static InetAddress ep4 = InetAddresses.forString("127.0.0.5");
    private static InetAddress ep5 = InetAddresses.forString("127.0.0.6");
    private static InetAddress ep6 = InetAddresses.forString("127.0.0.7");

    private static Token token0 = new BigIntegerToken("0");
    private static Token token0PlusOne = new BigIntegerToken("1");
    private static Token token0PlusTwo = new BigIntegerToken("2");
    private static Token tokenOneQuarter = new BigIntegerToken("42535295865117307932921825928971026432");
    private static Token tokenHalf = new BigIntegerToken("85070591730234615865843651857942052864");
    private static Token tokenHalfPlusOne = new BigIntegerToken("85070591730234615865843651857942052865");
    private static Token tokenThreeQuarters = new BigIntegerToken("127605887595351923798765477786913079296");

    private static Map<InetAddress, String> addressToDcMap = new ImmutableMap.Builder<InetAddress, String>()
            .put(ep0, DC1).put(ep1, DC2).put(ep2, DC2).put(ep3, DC3).put(ep4, DC3)
            .put(ep5, DC3).put(ep6, DC3).build();

    private static Map<InetAddress, String> addressToRackMap = new ImmutableMap.Builder<InetAddress, String>()
            .put(ep0, RAC1).put(ep1, RAC1).put(ep2, RAC2).put(ep3, RAC1).put(ep4, RAC1)
            .put(ep5, RAC2).put(ep6, RAC2).build();

    private static Set<Pair<Token, InetAddress>> sameDcRing = ImmutableSet.of(
            new Pair<Token, InetAddress>(token0, ep0), new Pair<Token, InetAddress>(tokenOneQuarter, ep1),
            new Pair<Token, InetAddress>(tokenHalf, ep2), new Pair<Token, InetAddress>(tokenThreeQuarters,
                    ep3));

    @SuppressWarnings("unchecked")
    private static Set<Pair<Token, InetAddress>> multiDcRing = ImmutableSet.of(
            new Pair<Token, InetAddress>(token0, ep0), new Pair<Token, InetAddress>(token0PlusOne, ep1),
            new Pair<Token, InetAddress>(tokenHalf, ep2), new Pair<Token, InetAddress>(token0PlusTwo, ep3),
            new Pair<Token, InetAddress>(tokenOneQuarter, ep4), new Pair<Token, InetAddress>(tokenHalfPlusOne, ep5),
            new Pair<Token, InetAddress>(tokenThreeQuarters, ep6));

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
    public void testOwnershipWithNoKeyspace() throws ConfigurationException, IOException
    {

        TokenMetadata metadata = new TokenMetadata();
        metadata.updateNormalTokens(sameDcRing);
        StorageService.instance.setTokenMetadataUnsafe(metadata);

        Table.open("Keyspace1").createReplicationStrategy(
                KSMetaData.newKeyspace("Keyspace1", SimpleStrategy.class.getName(),
                        ImmutableMap.of("replication_factor", "2")));

        StorageService.instance.setPartitionerUnsafe(new RandomPartitioner());
        Map<InetAddress, Float> ownership = StorageService.instance.getOwnership();

        assertEquals(4, ownership.size());
        for (Map.Entry<InetAddress, Float> entry : ownership.entrySet())
        {
            assertEquals("unexpected ring ownership", 0.25f, entry.getValue().floatValue(), 0);
        }
    }

    @Test
    public void testEffectiveOwnershipWithSimpleStrategy() throws ConfigurationException, IOException
    {

        TokenMetadata metadata = new TokenMetadata();
        metadata.updateNormalTokens(sameDcRing);
        StorageService.instance.setTokenMetadataUnsafe(metadata);

        Table.open("Keyspace1").createReplicationStrategy(
                KSMetaData.newKeyspace("Keyspace1", SimpleStrategy.class.getName(),
                        ImmutableMap.of("replication_factor", "2")));

        StorageService.instance.setPartitionerUnsafe(new RandomPartitioner());
        Map<InetAddress, Float> ownership = StorageService.instance.effectiveOwnership("Keyspace1");

        assertEquals(ownership.size(), 4);
        for (Map.Entry<InetAddress, Float> entry : ownership.entrySet())
        {
            assertEquals("unexpected ring ownership", 0.5f, entry.getValue().floatValue(), 0);
        }
    }

    @Test
    public void testEffectiveOwnershipWithNetworkTopologyStrategy() throws ConfigurationException, IOException
    {

        TokenMetadata metadata = new TokenMetadata();
        metadata.updateNormalTokens(multiDcRing);
        StorageService.instance.setTokenMetadataUnsafe(metadata);

        Table.open("Keyspace1").createReplicationStrategy(
                KSMetaData.newKeyspace("Keyspace1", NetworkTopologyStrategy.class.getName(),
                        ImmutableMap.of(DC1, "1", DC2, "1", DC3, "3")));

        StorageService.instance.setPartitionerUnsafe(new RandomPartitioner());
        Map<InetAddress, Float> ownership = StorageService.instance.effectiveOwnership("Keyspace1");

        assertEquals(7, ownership.size());

        assertEquals("unexpected token position", ep0,
                Iterables.get(ownership.keySet(), 0));
        assertEquals("unexpected token position", ep1,
                Iterables.get(ownership.keySet(), 1));
        assertEquals("unexpected token position", ep2,
                Iterables.get(ownership.keySet(), 2));
        assertEquals("unexpected token position", ep3,
                Iterables.get(ownership.keySet(), 3));
        assertEquals("unexpected token position", ep4,
                Iterables.get(ownership.keySet(), 4));
        assertEquals("unexpected token position", ep5,
                Iterables.get(ownership.keySet(), 5));
        assertEquals("unexpected token position", ep6,
                Iterables.get(ownership.keySet(), 6));

        assertEquals("unexpected ring ownership", 1f,
                Iterables.get(ownership.values(), 0), 0);
        assertEquals("unexpected ring ownership", 0.5f,
                Iterables.get(ownership.values(), 1), 0);
        assertEquals("unexpected ring ownership", 0.5f,
                Iterables.get(ownership.values(), 2), 0);
        assertEquals("unexpected ring ownership", 0.75f,
                Iterables.get(ownership.values(), 3), 0);
        assertEquals("unexpected ring ownership", 0.75f,
                Iterables.get(ownership.values(), 4), 0);
        assertEquals("unexpected ring ownership", 0.75f,
                Iterables.get(ownership.values(), 5), 0);
        assertEquals("unexpected ring ownership", 0.75f,
                Iterables.get(ownership.values(), 6), 0);
    }
}
