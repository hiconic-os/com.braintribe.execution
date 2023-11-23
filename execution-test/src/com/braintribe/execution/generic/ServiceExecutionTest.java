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
package com.braintribe.execution.generic;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.braintribe.utils.DateTools;
import com.braintribe.utils.date.NanoClock;

public class ServiceExecutionTest {

	@Test
	public void testStarted() throws Exception {
		
		ExecutorService service = Executors.newFixedThreadPool(1);
		
		int iterations = 10;
		long interval = 500L;
		List<ContextualizedFuture<Instant,String>> futures = new ArrayList<>();
		try {
			
			for (int i=0; i<iterations; ++i) {
				final Instant submitInstance = NanoClock.INSTANCE.instant();
				futures.add(ServiceExecution.submit(() -> {
					Thread.sleep(interval);
					return NanoClock.INSTANCE.instant();
				}, "Task started at "+DateTools.encode(submitInstance, DateTools.ISO8601_DATE_WITH_MS_FORMAT), service));
			}

			// Normally, the timeout of 1s would not be enough
			// But we are going to wait 10 s for the actual start and THEN 1 s, it should pass
			for (ContextualizedFuture<Instant,String> future : futures) {
				future.getWithRemainingTime(1000, TimeUnit.MILLISECONDS, Duration.ofMillis(10000L));
			}
			
		} finally {
			service.shutdown();
		}
	}
	
}
