/*
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

package org.apache.hudi.async;

import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AsyncRollbackService extends HoodieAsyncTableService {

  private static final Logger LOG = LogManager.getLogger(AsyncRollbackService.class);

  private final BaseHoodieWriteClient writeClient;
  private final transient ExecutorService executor = Executors.newSingleThreadExecutor();

  protected AsyncRollbackService(BaseHoodieWriteClient writeClient) {
    super(writeClient.getConfig());
    this.writeClient = writeClient;
  }

  @Override
  protected Pair<CompletableFuture, ExecutorService> startService() {
    String instantTime = HoodieActiveTimeline.createNewInstantTime();
    LOG.info(String.format("Starting async rollback service with instant time %s...", instantTime));
    return Pair.of(CompletableFuture.supplyAsync(() -> {
      writeClient.rollback(instantTime);
      return true;
    }, executor), executor);
  }

  public static AsyncRollbackService startAsyncRollbackIfEnabled(BaseHoodieWriteClient writeClient) {
    HoodieWriteConfig config = writeClient.getConfig();
    // TODO: add config for async rollback
    /*if (!config.isAutoClean() || !config.isAsyncClean()) {
      LOG.info("The HoodieWriteClient is not configured to auto & async rollback. Async rollback service will not start.");
      return null;
    }*/
    AsyncRollbackService asyncRollbackService = new AsyncRollbackService(writeClient);
    asyncRollbackService.start(null);
    return asyncRollbackService;
  }

  public static void waitForCompletion(AsyncRollbackService asyncRollbackService) {
    if (asyncRollbackService != null) {
      LOG.info("Waiting for async rollback service to finish");
      try {
        asyncRollbackService.waitForShutdown();
      } catch (Exception e) {
        throw new HoodieException("Error waiting for async rollback service to finish", e);
      }
    }
  }

  public static void forceShutdown(AsyncRollbackService asyncRollbackService) {
    if (asyncRollbackService != null) {
      LOG.info("Shutting down async rollback service...");
      asyncRollbackService.shutdown(true);
    }
  }
}
