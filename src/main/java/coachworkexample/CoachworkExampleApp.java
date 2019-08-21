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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import com.hotels.chassis.Chassis;
import com.hotels.chassis.ChassisFunction;

@SpringBootApplication
public class CoachworkExampleApp {

  @Bean
  public Class<WorkerArgument> workerArgumentType() {
    return WorkerArgument.class;
  }

  @Bean
  public Class<WorkerResult> workerResultType() {
    return WorkerResult.class;
  }

  @Bean
  public ChassisFunction<WorkerArgument, WorkerResult> workerFunction() {
    return new WorkerFunction();
  }

  @Bean(initMethod = "start")
  public CuratorFramework curator(@Value("${zookeeper.connect}") String zkConnectString) {
    return CuratorFrameworkFactory.newClient(zkConnectString, new ExponentialBackoffRetry(100000, 10));
  }

  @Bean
  public Chassis<WorkerArgument, WorkerResult> chassis(CuratorFramework curator) throws Exception {
    return Chassis.builder(workerArgumentType(), workerResultType()).curator(curator).worker(workerFunction()).build();
  }

  public static void main(String[] args) throws Exception {
    SpringApplication.run(CoachworkExampleApp.class, args);
  }
}
