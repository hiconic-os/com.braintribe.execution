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
