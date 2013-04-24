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

var numMessages = 10;

function testPersistentReloadWorkQueue() {

  var count = 0;
  var doneHandler = function() {
    if (++count == numMessages) {
      vassert.testComplete();
    }
  };

  eb.registerHandler("done", doneHandler);

  var persistorConfig = {address: 'test.persistor', db_name: 'test_db', fake: true}
  container.deployModule('io.vertx~mod-mongo-persistor~2.0.0-SNAPSHOT', persistorConfig, function(err, deployID) {
    insertWork();
    var queueConfig = {address: 'test.orderQueue', persistor_address: 'test.persistor', collection: 'work'}
    container.deployModule(java.lang.System.getProperty("vertx.modulename"), queueConfig, function(err, deployID) {
      container.deployVerticle('integration_tests/javascript/order_processor.js', 10);
    });
  });
}

function insertWork() {

  for (var i = 0; i < numMessages; i++) {
    eb.send('test.persistor', {
      collection: 'work',
      action: 'save',
      document: {
        blah: "foo" + i
      }
    });
  }
}

vertxTests.startTests(this)