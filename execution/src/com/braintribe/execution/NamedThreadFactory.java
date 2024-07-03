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
package com.braintribe.execution;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This minor variation of the {@link Executors.DefaultThreadFactory} does exactly the same but tries - in addition - to
 * find a meaningful name for the thread (for improved debugging and analytics).
 * 
 * @author roman.kurmanowytsch
 */
public class NamedThreadFactory implements ThreadFactory {

	protected boolean doLogging = false;

	static final AtomicInteger poolNumber = new AtomicInteger(1);
	final AtomicInteger threadNumber = new AtomicInteger(1);
	private String namePrefix;

	private int priority = Thread.NORM_PRIORITY;

	public NamedThreadFactory() {
		this.namePrefix = "pool-" + poolNumber.getAndIncrement() + "-thread-";
	}

	@Override
	public Thread newThread(final Runnable r) {

		final Thread t = Thread.ofVirtual().name(this.namePrefix + this.threadNumber.getAndIncrement()).unstarted(r);
		if (t.getPriority() != this.priority) {
			t.setPriority(this.priority);
		}
		return t;
	}

	public boolean isDoLogging() {
		return this.doLogging;
	}

	public void setDoLogging(final boolean doLogging) {
		this.doLogging = doLogging;
	}
	public void setNamePrefix(String namePrefix) {
		if (namePrefix == null || namePrefix.isEmpty()) {
			return;
		}
		if (!namePrefix.endsWith("-")) {
			namePrefix = namePrefix + "-";
		}
		this.namePrefix = namePrefix;
	}
	public void setPriority(int priority) {
		this.priority = priority;
	}

}
