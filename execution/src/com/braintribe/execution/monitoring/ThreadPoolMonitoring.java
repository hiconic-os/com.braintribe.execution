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
