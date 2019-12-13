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
package com.huaweicloud.nosql.streamfetcher.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


public class UUIDGen
{
    // A grand day! millis at 00:00:00.000 15 Oct 1582.
    private static final long START_EPOCH = -12219292800000L;
    private static final long clockSeqAndNode = makeClockSeqAndNode();

    // placement of this singleton is important.  It needs to be instantiated *AFTER* the other statics.
    private static final UUIDGen instance = new UUIDGen();

    private UUIDGen()
    {
        // make sure someone didn't whack the clockSeqAndNode by changing the order of instantiation.
        if (clockSeqAndNode == 0) throw new RuntimeException("singleton instantiation is misplaced.");
    }

    /**
     * Creates a type 1 UUID (time-based UUID) with the timestamp of @param when, in milliseconds.
     *
     * @return a UUID instance
     */
    public static UUID getTimeUUID_V2(long when)
    {
        return new UUID(instance.createTimeSafe_V2(when), clockSeqAndNode);
    }

    private static long makeClockSeqAndNode()
    {
        long clock = new SecureRandom().nextLong();

        long lsb = 0;
        lsb |= 0x8000000000000000L;                 // variant (2 bits)
        lsb |= (clock & 0x0000000000003FFFL) << 48; // clock sequence (14 bits)
        lsb |= makeNode();                          // 6 bytes
        return lsb;
    }

    private long createTimeSafe_V2(long when)
    {
        long newLastNanos =  (when - START_EPOCH) * 10000;
        return createTime(newLastNanos);
    }

    private static long createTime(long nanosSince)
    {
        long msb = 0L;
        msb |= (0x00000000ffffffffL & nanosSince) << 32;
        msb |= (0x0000ffff00000000L & nanosSince) >>> 16;
        msb |= (0xffff000000000000L & nanosSince) >>> 48;
        msb |= 0x0000000000001000L; // sets the version to 1.
        return msb;
    }

    private static long makeNode()
    {
        Collection<InetAddress> localAddresses = getAllLocalAddresses();
        if (localAddresses.isEmpty())
            throw new RuntimeException("Cannot generate the node component of the UUID because cannot retrieve any IP addresses.");

        // ideally, we'd use the MAC address, but java doesn't expose that.
        byte[] hash = hash(localAddresses);
        long node = 0;
        for (int i = 0; i < Math.min(6, hash.length); i++)
            node |= (0x00000000000000ff & (long)hash[i]) << (5-i)*8;
        assert (0xff00000000000000L & node) == 0;

        // Since we don't use the mac address, the spec says that multicast
        // bit (least significant bit of the first octet of the node ID) must be 1.
        return node | 0x0000010000000000L;
    }

    private static byte[] hash(Collection<InetAddress> data)
    {
        try
        {
            // Identify the host.
            MessageDigest messageDigest = MessageDigest.getInstance("MD5");
            for(InetAddress addr : data)
                messageDigest.update(addr.getAddress());

            // Identify the process on the load: we use both the PID and class loader hash.
//            long pid = NativeLibrary.getProcessID();
            long pid = -1;
            if (pid < 0)
                pid = new Random(System.currentTimeMillis()).nextLong();
            updateWithLong(messageDigest, pid);

            ClassLoader loader = UUIDGen.class.getClassLoader();
            int loaderId = loader != null ? System.identityHashCode(loader) : 0;
            updateWithInt(messageDigest, loaderId);

            return messageDigest.digest();
        }
        catch (NoSuchAlgorithmException nsae)
        {
            throw new RuntimeException("MD5 digest algorithm is not available", nsae);
        }
    }

    public static Collection<InetAddress> getAllLocalAddresses()
    {
        Set<InetAddress> localAddresses = new HashSet<InetAddress>();
        try
        {
            Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
            if (nets != null)
            {
                while (nets.hasMoreElements())
                    localAddresses.addAll(Collections.list(nets.nextElement().getInetAddresses()));
            }
        }
        catch (SocketException e)
        {
            throw new AssertionError(e);
        }
        return localAddresses;
    }

    public static void updateWithInt(MessageDigest digest, int val)
    {
        digest.update((byte) ((val >>> 24) & 0xFF));
        digest.update((byte) ((val >>> 16) & 0xFF));
        digest.update((byte) ((val >>>  8) & 0xFF));
        digest.update((byte) ((val >>> 0) & 0xFF));
    }

    public static void updateWithLong(MessageDigest digest, long val)
    {
        digest.update((byte) ((val >>> 56) & 0xFF));
        digest.update((byte) ((val >>> 48) & 0xFF));
        digest.update((byte) ((val >>> 40) & 0xFF));
        digest.update((byte) ((val >>> 32) & 0xFF));
        digest.update((byte) ((val >>> 24) & 0xFF));
        digest.update((byte) ((val >>> 16) & 0xFF));
        digest.update((byte) ((val >>>  8) & 0xFF));
        digest.update((byte)  ((val >>> 0) & 0xFF));
    }

    public static void main(String[] args) {
//        System.out.println(getTimeUUID(1576050457));
        long timePoint = System.currentTimeMillis() - Config.GRACE_TIME * 1000;
        UUID timeUUID = UUIDGen.getTimeUUID_V2(timePoint);
        System.out.println(timeUUID);
    }
}

