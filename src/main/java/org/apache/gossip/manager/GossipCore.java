/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gossip.manager;

import common.Constants;
import common.IDataNode;
import common.IRoutingTable;
import org.apache.gossip.GossipMember;
import org.apache.gossip.LocalGossipMember;
import org.apache.gossip.RemoteGossipMember;
import org.apache.gossip.crdt.Crdt;
import org.apache.gossip.event.GossipState;
import org.apache.gossip.model.*;
import org.apache.gossip.udp.Trackable;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.URI;
import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.*;

public class GossipCore implements GossipCoreConstants {

    public static final Logger LOGGER = Logger.getLogger(GossipCore.class);
    private final GossipManager gossipManager;
    public final IDataNode dataNode;
    private ConcurrentHashMap<String, Base> requests;
    private ThreadPoolExecutor service;
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, GossipDataMessage>> perNodeData;
    private final ConcurrentHashMap<String, SharedGossipDataMessage> sharedData;
    private final BlockingQueue<Runnable> workQueue;
    private final PKCS8EncodedKeySpec privKeySpec;
    private final PrivateKey privKey;

    public GossipCore(GossipManager manager, IDataNode dataNode) {
        this.gossipManager = manager;
        requests = new ConcurrentHashMap<>();
        workQueue = new ArrayBlockingQueue<>(1024);
        service = new ThreadPoolExecutor(1, 5, 1, TimeUnit.SECONDS, workQueue, new ThreadPoolExecutor.DiscardOldestPolicy());
        perNodeData = new ConcurrentHashMap<>();
        sharedData = new ConcurrentHashMap<>();
        this.dataNode = dataNode;

        if (manager.getSettings().isSignMessages()) {
            File privateKey = new File(manager.getSettings().getPathToKeyStore(), manager.getMyself().getId());
            File publicKey = new File(manager.getSettings().getPathToKeyStore(), manager.getMyself().getId() + ".pub");
            if (!privateKey.exists()) {
                throw new IllegalArgumentException("private key not found " + privateKey);
            }
            if (!publicKey.exists()) {
                throw new IllegalArgumentException("public key not found " + publicKey);
            }
            try (FileInputStream keyfis = new FileInputStream(privateKey)) {
                byte[] encKey = new byte[keyfis.available()];
                keyfis.read(encKey);
                keyfis.close();
                privKeySpec = new PKCS8EncodedKeySpec(encKey);
                KeyFactory keyFactory = KeyFactory.getInstance("DSA");
                privKey = keyFactory.generatePrivate(privKeySpec);
            } catch (NoSuchAlgorithmException | InvalidKeySpecException | IOException e) {
                throw new RuntimeException("failed hard", e);
            }
        } else {
            privKeySpec = null;
            privKey = null;
        }
    }

    private byte[] sign(byte[] bytes) {
        Signature dsa;
        try {
            dsa = Signature.getInstance("SHA1withDSA", "SUN");
            dsa.initSign(privKey);
            dsa.update(bytes);
            return dsa.sign();
        } catch (NoSuchAlgorithmException | NoSuchProviderException | InvalidKeyException | SignatureException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public void addSharedData(SharedGossipDataMessage message) {
        while (true) {
            SharedGossipDataMessage previous = sharedData.putIfAbsent(message.getKey(), message);
            if (previous == null) {
                return;
            }
            if (previous.getTimestamp() < message.getTimestamp()) {
                boolean result = sharedData.replace(message.getKey(), previous, message);
                if (Constants.ROUTING_TABLE.equals(message.getKey())) {
                    dataNode.UpdateRoutingTable((IRoutingTable) message.getPayload());
                }
                if (result) {
                    return;
                }
            } else {
                return;
            }
        }
    }

    public void addPerNodeData(GossipDataMessage message) {
        ConcurrentHashMap<String, GossipDataMessage> nodeMap = new ConcurrentHashMap<>();
        nodeMap.put(message.getKey(), message);
        nodeMap = perNodeData.putIfAbsent(message.getNodeId(), nodeMap);
        if (nodeMap != null) {
            GossipDataMessage current = nodeMap.get(message.getKey());
            if (current == null) {
                nodeMap.putIfAbsent(message.getKey(), message);
            } else {
                if (current.getTimestamp() < message.getTimestamp()) {
                    nodeMap.replace(message.getKey(), current, message);
                }
            }
        }
    }

    public ConcurrentHashMap<String, ConcurrentHashMap<String, GossipDataMessage>> getPerNodeData() {
        return perNodeData;
    }

    public ConcurrentHashMap<String, SharedGossipDataMessage> getSharedData() {
        return sharedData;
    }

    public void shutdown() {
        service.shutdown();
        try {
            service.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOGGER.warn(e);
        }
        service.shutdownNow();
    }

    public void receive(Base base) {
        if (!gossipManager.getMessageInvoker().invoke(this, gossipManager, base)) {
            LOGGER.warn("received message can not be handled");
        }
    }

    /**
     * Sends a blocking message.
     *
     * @param message
     * @param uri
     * @throws RuntimeException if data can not be serialized or in transmission error
     */
    private void sendInternal(Base message, URI uri) {
        byte[] json_bytes;
        try {
            if (privKey == null) {
                json_bytes = gossipManager.getObjectMapper().writeValueAsBytes(message);
            } else {
                SignedPayload p = new SignedPayload();
                p.setData(gossipManager.getObjectMapper().writeValueAsString(message).getBytes());
                p.setSignature(sign(p.getData()));
                json_bytes = gossipManager.getObjectMapper().writeValueAsBytes(p);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try (DatagramSocket socket = new DatagramSocket()) {
            socket.setSoTimeout(gossipManager.getSettings().getGossipInterval() * 2);
            InetAddress dest = InetAddress.getByName(uri.getHost());
            DatagramPacket datagramPacket = new DatagramPacket(json_bytes, json_bytes.length, dest, uri.getPort());
            socket.send(datagramPacket);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Response send(Base message, URI uri) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Sending " + message);
            LOGGER.debug("Current request queue " + requests);
        }

        final Trackable t;
        if (message instanceof Trackable) {
            t = (Trackable) message;
        } else {
            t = null;
        }
        sendInternal(message, uri);
        if (t == null) {
            return null;
        }
        final Future<Response> response = service.submit(new Callable<Response>() {
            @Override
            public Response call() throws Exception {
                while (true) {
                    Base b = requests.remove(t.getUuid() + "/" + t.getUriFrom());
                    if (b != null) {
                        return (Response) b;
                    }
                    try {
                        Thread.sleep(0, 555555);
                    } catch (InterruptedException e) {

                    }
                }
            }
        });

        try {
            //TODO this needs to be a setting base on attempts/second
            return response.get(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            LOGGER.debug(e.getMessage(), e);
            return null;
        } catch (TimeoutException e) {
            boolean cancelled = response.cancel(true);
            LOGGER.debug(String.format("Threadpool timeout attempting to contact %s, cancelled ? %b", uri.toString(), cancelled));
            return null;
        } finally {
            if (t != null) {
                requests.remove(t.getUuid() + "/" + t.getUriFrom());
            }
        }
    }

    /**
     * Sends a message across the network while blocking. Catches and ignores IOException in transmission. Used
     * when the protocol for the message is not to wait for a response
     *
     * @param message the message to send
     * @param u       the uri to send it to
     */
    public void sendOneWay(Base message, URI u) {
        byte[] json_bytes;
        try {
            if (privKey == null) {
                json_bytes = gossipManager.getObjectMapper().writeValueAsBytes(message);
            } else {
                SignedPayload p = new SignedPayload();
                p.setData(gossipManager.getObjectMapper().writeValueAsString(message).getBytes());
                p.setSignature(sign(p.getData()));
                json_bytes = gossipManager.getObjectMapper().writeValueAsBytes(p);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try (DatagramSocket socket = new DatagramSocket()) {
            socket.setSoTimeout(gossipManager.getSettings().getGossipInterval() * 2);
            InetAddress dest = InetAddress.getByName(u.getHost());
            DatagramPacket datagramPacket = new DatagramPacket(json_bytes, json_bytes.length, dest, u.getPort());
            socket.send(datagramPacket);
        } catch (IOException ex) {
            LOGGER.debug("Send one way failed", ex);
        }
    }

    public void addRequest(String k, Base v) {
        requests.put(k, v);
    }

    /**
     * Merge lists from remote members and update heartbeats
     *
     * @param gossipManager
     * @param senderMember
     * @param remoteList
     */
    public void mergeLists(GossipManager gossipManager, RemoteGossipMember senderMember,
                           List<GossipMember> remoteList) {
        if (LOGGER.isDebugEnabled()) {
            debugState(senderMember, remoteList);
        }
        for (LocalGossipMember i : gossipManager.getDeadMembers()) {
            if (i.getId().equals(senderMember.getId())) {
                LOGGER.debug(gossipManager.getMyself() + " contacted by dead member " + senderMember.getUri());
                i.recordHeartbeat(senderMember.getHeartbeat());
                i.setHeartbeat(senderMember.getHeartbeat());
                //TODO consider forcing an UP here
            }
        }
        for (GossipMember remoteMember : remoteList) {
            if (remoteMember.getId().equals(gossipManager.getMyself().getId())) {
                continue;
            }
            LocalGossipMember aNewMember = new LocalGossipMember(remoteMember.getClusterName(),
                    remoteMember.getUri(),
                    remoteMember.getId(),
                    remoteMember.getHeartbeat(),
                    remoteMember.getProperties(),
                    gossipManager.getSettings().getWindowSize(),
                    gossipManager.getSettings().getMinimumSamples(),
                    gossipManager.getSettings().getDistribution());
            aNewMember.recordHeartbeat(remoteMember.getHeartbeat());
            Object result = gossipManager.getMembers().putIfAbsent(aNewMember, GossipState.UP);
            if (result != null) {
                for (Entry<LocalGossipMember, GossipState> localMember : gossipManager.getMembers().entrySet()) {
                    if (localMember.getKey().getId().equals(remoteMember.getId())) {
                        localMember.getKey().recordHeartbeat(remoteMember.getHeartbeat());
                        localMember.getKey().setHeartbeat(remoteMember.getHeartbeat());
                        localMember.getKey().setProperties(remoteMember.getProperties());
                    }
                }
            }
        }
        if (LOGGER.isDebugEnabled()) {
            debugState(senderMember, remoteList);
        }
    }

    private void debugState(RemoteGossipMember senderMember,
                            List<GossipMember> remoteList) {
        LOGGER.warn(
                "-----------------------\n" +
                        "Me " + gossipManager.getMyself() + "\n" +
                        "Sender " + senderMember + "\n" +
                        "RemoteList " + remoteList + "\n" +
                        "Live " + gossipManager.getLiveMembers() + "\n" +
                        "Dead " + gossipManager.getDeadMembers() + "\n" +
                        "=======================");
    }

    @SuppressWarnings("rawtypes")
    public Crdt merge(SharedGossipDataMessage message) {
        for (; ; ) {
            SharedGossipDataMessage previous = sharedData.putIfAbsent(message.getKey(), message);
            if (previous == null) {
                return (Crdt) message.getPayload();
            }
            SharedGossipDataMessage copy = new SharedGossipDataMessage();
            copy.setExpireAt(message.getExpireAt());
            copy.setKey(message.getKey());
            copy.setNodeId(message.getNodeId());
            copy.setTimestamp(message.getTimestamp());
            Crdt merged = ((Crdt) previous.getPayload()).merge((Crdt) message.getPayload());
            copy.setPayload(merged);
            boolean replaced = sharedData.replace(message.getKey(), previous, copy);
            if (replaced) {
                return merged;
            }
        }
    }
}
