// ============================================================================
// Copyright BRAINTRIBE TECHNOLOGY GMBH, Austria, 2002-2022
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// ============================================================================
package com.braintribe.execution.monitoring;

import java.util.concurrent.ForkJoinPool;

import com.braintribe.utils.RandomTools;

public class CommonPoolThreadPoolStatistics implements ThreadPoolStatistics {

	public static CommonPoolThreadPoolStatistics commonPoolStatistics = new CommonPoolThreadPoolStatistics();

	private String id = RandomTools.newStandardUuid();

	@Override
	public int currentlyRunning() {
		return ForkJoinPool.commonPool().getRunningThreadCount();
	}

	@Override
	public long totalExecutions() {
		return -1;
	}

	@Override
	public long averageRunningTimeInMs() {
		return -1L;
	}

	@Override
	public String getThreadPoolId() {
		return id;
	}

	@Override
	public int getPendingTasksInQueue() {
		return (int) ForkJoinPool.commonPool().getQueuedTaskCount();
	}

	@Override
	public long timeSinceLastExecutionInMs() {
		return -1;
	}

	@Override
	public int getPoolSize() {
		return ForkJoinPool.commonPool().getRunningThreadCount();
	}

	@Override
	public int getCorePoolSize() {
		return ForkJoinPool.commonPool().getPoolSize();
	}

	@Override
	public int getMaximumPoolSize() {
		return ForkJoinPool.commonPool().getPoolSize();
	}

	@Override
	public String getDescription() {
		return "Java Common Pool";
	}

	@Override
	public boolean isScheduledThreadPool() {
		return false;
	}

	@Override
	public Long getMaximumEnqueuedTimeInMs() {
		return null;
	}

	@Override
	public Long getMinimumEnqueuedTimeInMs() {
		return null;
	}

	@Override
	public Double getAverageEnqueuedTimeInMs() {
		return null;
	}

}
