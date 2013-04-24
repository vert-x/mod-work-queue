/*
 * Copyright 2011-2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

var container = require("container");
var vertx = require("vertx")
var vertxTests = require("vertx_tests");
var vassert = require("vertx_assert");

var eb = vertx.eventBus;

function testPersistentWorkQueue() {

  var numMessages = 100;

  var count = 0;
  var doneHandler = function() {
    if (++count == numMessages) {
      eb.unregisterHandler("done", doneHandler);
      vassert.testComplete();
    }
  };

  eb.registerHandler("done", doneHandler);

  for (var i = 0; i < numMessages; i++) {
    eb.send('test.orderQueue', {
      blah: "somevalue: " + i
    })
  }

}

function deleteAll() {
  eb.send('test.persistor', {
    collection: 'work',
    action: 'delete',
    matcher: {}
  }, function(reply) {
    vassert.assertEquals("ok", reply.status);
  });
}

var persistorConfig = {address: 'test.persistor', db_name: 'test_db', fake: true}
var script = this;
var numProcessors = 10;
container.deployModule('io.vertx~mod-mongo-persistor~2.0.0-SNAPSHOT', persistorConfig, function(err, deployID) {
  vassert.assertTrue(err === null);
  deleteAll();
  var queueConfig = {address: 'test.orderQueue', persistor_address: 'test.persistor', collection: 'work'}
  container.deployModule(java.lang.System.getProperty("vertx.modulename"), queueConfig, function(err, deployID) {
    vassert.assertTrue(err === null);
    container.deployVerticle("integration_tests/javascript/order_processor.js", numProcessors, function(err, depID) {
      vassert.assertTrue(err === null);
      vertxTests.startTests(script);
    })
  });
});
