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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class ThreadPoolMonitoring {

	private static ConcurrentHashMap<String, StandardThreadPoolStatistics> threadPools = new ConcurrentHashMap<>();

	public static void registerThreadPool(final String threadPoolId, final MonitoredThreadPool extendedThreadPoolExecutor) {
		if (threadPoolId == null) {
			return;
		}
		StandardThreadPoolStatistics statistics = threadPools.computeIfAbsent(threadPoolId,
				id -> new StandardThreadPoolStatistics(threadPoolId, extendedThreadPoolExecutor));
		if (extendedThreadPoolExecutor instanceof ScheduledThreadPoolExecutor) {
			statistics.setScheduledThreadPool(true);
		}
	}

	public static void unregisterThreadPool(final String threadPoolId) {
		if (threadPoolId == null) {
			return;
		}
		threadPools.remove(threadPoolId);
	}

	public static void beforeExecution(String threadPoolId, String execIdString) {
		if (threadPoolId == null || execIdString == null) {
			return;
		}
		StandardThreadPoolStatistics statistics = threadPools.get(threadPoolId);
		if (statistics != null) {
			statistics.beforeExecution(execIdString);
		}
	}

	public static void afterExecution(String threadPoolId, String execIdString) {
		if (threadPoolId == null || execIdString == null) {
			return;
		}
		StandardThreadPoolStatistics statistics = threadPools.get(threadPoolId);
		if (statistics != null) {
			statistics.afterExecution(execIdString);
		}

	}

	public static List<ThreadPoolStatistics> getStatistics() {
		ArrayList<ThreadPoolStatistics> list = new ArrayList<>(threadPools.size() + 1);
		list.add(CommonPoolThreadPoolStatistics.commonPoolStatistics);
		list.addAll(threadPools.values());
		return list;
	}

	public static void registerThreadPoolExecution(String threadPoolId, Duration enqueued, Duration execution) {
		StandardThreadPoolStatistics statistics = threadPools.get(threadPoolId);
		if (statistics != null) {
			statistics.registerThreadPoolExecution(enqueued, execution);
		}
	}
}
