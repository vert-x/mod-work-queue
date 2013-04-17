package org.vertx.mods.workqueue.test.integration.java;

import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;

import static org.vertx.testtools.VertxAssert.assertEquals;
import static org.vertx.testtools.VertxAssert.testComplete;

/*
 * Copyright 2013 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public class WorkQueueTest extends TestVerticle {

  EventBus eb;
  int processedCount;
  int acceptedCount;
  final int numProcessors = 10;

  @Override
  public void start() {
    eb = vertx.eventBus();
    JsonObject config = new JsonObject();
    config.putString("address", "test.orderQueue");

    container.deployModule(System.getProperty("vertx.modulename"), config, 1, new AsyncResultHandler<String>() {
      public void handle(AsyncResult<String> asyncResult) {
        if (asyncResult.succeeded()) {
          container.deployVerticle(OrderProcessor.class.getName(), null, numProcessors, new AsyncResultHandler<String>() {
            @Override
            public void handle(AsyncResult<String> asyncResult1) {
              if (asyncResult1.succeeded()) {
                WorkQueueTest.super.start();
              } else {
                asyncResult1.cause().printStackTrace();
              }
            }
          });
        } else {
          asyncResult.cause().printStackTrace();
        }
      }
    });
  }

  @Test
  public void testSimple() throws Exception {

    final int numMessages = 30;

    eb.registerHandler("done", new Handler<Message<JsonObject>>() {
      public void handle(Message<JsonObject> message) {
        if (++processedCount == numMessages) {
          testComplete();
        }
      }
    });

    for (int i = 0; i < numMessages; i++) {
      JsonObject obj = new JsonObject().putString("blah", "wibble" + i);
      eb.send("test.orderQueue", obj);
    }
  }

  @Test
  public void testWithAcceptedReply() throws Exception {

    final int numMessages = 30;

    eb.registerHandler("accepted", new Handler<Message<JsonObject>>() {
      public void handle(Message<JsonObject> message) {
        acceptedCount++;
      }
    });

    eb.registerHandler("done", new Handler<Message<JsonObject>>() {
      public void handle(Message<JsonObject> message) {
        if (++processedCount == numMessages) {
          assertEquals(acceptedCount, numMessages);
          testComplete();
        }
      }
    });

    for (int i = 0; i < numMessages; i++) {
      JsonObject obj = new JsonObject().putString("blah", "wibble" + i).putString("accepted-reply", "accepted");
      eb.send("test.orderQueue", obj);
    }
  }
}
