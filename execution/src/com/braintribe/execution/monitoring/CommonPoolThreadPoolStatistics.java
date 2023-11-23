// ============================================================================
// Copyright BRAINTRIBE TECHNOLOGY GMBH, Austria, 2002-2022
// 
// This library is free software; you can redistribute it and/or modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either version 3 of the License, or (at your option) any later version.
// 
// This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public License along with this library; See http://www.gnu.org/licenses/.
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
