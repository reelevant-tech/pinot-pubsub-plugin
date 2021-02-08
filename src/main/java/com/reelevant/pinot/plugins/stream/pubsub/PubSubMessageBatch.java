/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.reelevant.pinot.plugins.stream.pubsub;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import com.google.pubsub.v1.ReceivedMessage;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;


public class PubSubMessageBatch implements MessageBatch<byte[]> {

  private List<MessageAndOffset> messageList = new ArrayList<MessageAndOffset>();
  private final Consumer<ArrayList<String>> ackCallback;
  private ArrayList<String> ackIds = new ArrayList<String>();

  public PubSubMessageBatch(Iterator<ReceivedMessage> it, Long startOffset, Consumer<ArrayList<String>> callback) {
    this.ackCallback = callback;
    while (it.hasNext()) {
      ReceivedMessage message = it.next();
      ackIds.add(message.getAckId());
      messageList.add(new MessageAndOffset(message.getMessage().getData().toByteArray(), startOffset + messageList.size()));
    }
  }

  @Override
  public int getMessageCount() {
    return messageList.size();
  }

  @Override
  public byte[] getMessageAtIndex(int index) {
    return messageList.get(index).getMessage().array();
  }

  @Override
  public int getMessageOffsetAtIndex(int index) {
    return messageList.get(index).getMessage().arrayOffset();
  }

  @Override
  public int getMessageLengthAtIndex(int index) {
    return messageList.get(index).payloadSize();
  }

  @Override
  public long getNextStreamMessageOffsetAtIndex(int index) {
    throw new UnsupportedOperationException("This method is deprecated");
  }

  @Override
  public StreamPartitionMsgOffset getNextStreamParitionMsgOffsetAtIndex(int index) {
    // when 
    if (index + 1 == this.getMessageCount()) {
      this.ackCallback.accept(this.ackIds);
    }
    return new LongMsgOffset(messageList.get(index).getNextOffset());
  }
}
