/**
 * Copyright (C) 2018 Expedia Inc.
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
package coachworkexample;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import javax.annotation.PostConstruct;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.springframework.stereotype.Component;

import com.hotels.chassis.Chassis;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Slf4j
@Component
public class LeaderService extends LeaderSelectorListenerAdapter implements AutoCloseable {

  private final Chassis<WorkerArgument, WorkerResult> chassis;

  private final LeaderSelector leaderSelector;

  private volatile boolean isRunning = true;

  public LeaderService(CuratorFramework curator, Chassis<WorkerArgument, WorkerResult> chassis) {
    this.chassis = chassis;
    leaderSelector = new LeaderSelector(curator, "/leader", this);
  }

  @PostConstruct
  public void start() {
    leaderSelector.start();
  }

  @Override
  public void takeLeadership(CuratorFramework client) throws Exception {
    log.info("Leader service starting");
    try {
      int n = 1;
      while (isRunning) {
        if (n > 1) { continue; }
        n++;
        Thread.sleep(10);
        List<WorkerArgument> l = new ArrayList<>();
        l.add(new WorkerArgument(1));
        l.add(new WorkerArgument(2));
        l.add(new WorkerArgument(3));
        l.add(new WorkerArgument(4));
        l.add(new WorkerArgument(5));
        l.add(new WorkerArgument(6));
        CompletableFuture<List<WorkerResult>> futureResults = completeJobs(l);

        futureResults.whenCompleteAsync((resultsList, e) -> {
          if (e != null) {
            log.info(String.format("Failed!"));
          } else {
            log.info("Landing {}: Succeeded!");
          }
        });
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      log.warn("Leader threw exception", e);
      throw e;
    }
    log.info("Leader service stopped");
  }

  @Override
  public void close() throws Exception {
    leaderSelector.close();
    isRunning = false;
  }

  public CompletableFuture<List<WorkerResult>> completeJobs(List<WorkerArgument> jobArgs) {
    return Flux
        .fromIterable(jobArgs)
        .flatMap(arg -> Mono
            .just(arg)
            .map(this::applyRunner)
            .flatMap(Mono::fromCompletionStage)
            .doOnError(t -> System.out.println("Error1"))
            .retry(3)
            .timeout(Duration.ofMillis(10000))
            .doOnError(t -> System.out.println("Error2")))
        .collectList()
        .toFuture();
  }

  private CompletableFuture<WorkerResult> applyRunner(WorkerArgument t) {
    return chassis.apply(t);
  }
}
