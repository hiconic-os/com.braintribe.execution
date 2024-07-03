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

import java.time.Instant;

import com.braintribe.utils.date.NanoClock;

public class MonitoringRunnable implements Runnable {

	private Runnable delegate;
	private Instant creationInstant;
	private ExecutionMonitoring monitoring;

	public MonitoringRunnable(Runnable delegate, ExecutionMonitoring monitoring) {
		this.monitoring = monitoring;
		this.creationInstant = NanoClock.INSTANCE.instant();
		this.delegate = delegate;
	}

	@Override
	public void run() {
		Instant executionInstant = NanoClock.INSTANCE.instant();
		try {
			this.delegate.run();
		} finally {
			Instant finishedInstant = NanoClock.INSTANCE.instant();
			monitoring.accept(creationInstant, executionInstant, finishedInstant);
		}
	}
}
