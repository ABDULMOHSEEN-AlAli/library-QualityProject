/**
 * Copyright (c) 2007-2013 Alysson Bessani, Eduardo Alchieri, Paulo Sousa, and the authors indicated in the @author tags
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package bftsmart.demo.ycsb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import bftsmart.tom.ServiceProxy;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;

/**
 *
 * @author Marcel Santos
 *
 */
public class YCSBClient extends DB {

    private static AtomicInteger counter = new AtomicInteger();
    private ServiceProxy proxy = null;
    private int myId;

    public YCSBClient() {
    }

    @Override
    public void init() {
        Properties props = getProperties();
        int initId = Integer.valueOf((String) props.get("smart-initkey"));
        myId = initId + counter.addAndGet(1);
        proxy = new ServiceProxy(myId);
        System.out.println("YCSBKVClient. Initiated client id: " + myId);
    }

    @Override
    public int delete(String arg0, String arg1) {
        throw new UnsupportedOperationException();
    }
    /**
     * Handles both insert and update operations by constructing a message based on the operation type
     * and sending it through the proxy.
     */
    private int handleInsertOrUpdate(String operationType, String table, String key, HashMap<String, ByteIterator> values) {
        // Convert values to a HashMap<String, byte[]> format
        HashMap<String, byte[]> map = new HashMap<>();
        for (String field : values.keySet()) {
            map.put(field, values.get(field).toArray());
        }

        // Create the message based on the operation type
        YCSBMessage msg = operationType.equals("insert") ?
                YCSBMessage.newInsertRequest(table, key, map) :
                YCSBMessage.newUpdateRequest(table, key, map);

        // Send the message and process the response
        byte[] reply = proxy.invokeOrdered(msg.getBytes());
        YCSBMessage replyMsg = YCSBMessage.getObject(reply);
        return replyMsg.getResult();
    }

    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        return handleInsertOrUpdate("insert", table, key, values);
    }

    @Override
    public int read(String table, String key,
            Set<String> fields, HashMap<String, ByteIterator> result) {
        HashMap<String, byte[]> results = new HashMap<>();
        YCSBMessage request = YCSBMessage.newReadRequest(table, key, fields, results);
        byte[] reply = proxy.invokeUnordered(request.getBytes());
        YCSBMessage replyMsg = YCSBMessage.getObject(reply);
        return replyMsg.getResult();
    }

    @Override
    public int scan(String arg0, String arg1, int arg2, Set<String> arg3,
            Vector<HashMap<String, ByteIterator>> arg4) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        return handleInsertOrUpdate("update", table, key, values);
    }

}
