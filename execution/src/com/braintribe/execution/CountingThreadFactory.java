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

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import com.braintribe.cfg.Configurable;

public class CountingThreadFactory implements ThreadFactory {

	private static final AtomicInteger poolNumber = new AtomicInteger(1);

	private final ThreadGroup group;
	private final String namePrefix;
	private final AtomicInteger threadNumber = new AtomicInteger(1);

	// Please do not change this to a virtual thread factory
	// As of today (26.6.25) virtual threads are dangerous and can cause deadlocks
	// Their usage should be well thought out, not default
	private ExtendedThreadFactory factory = Thread::new;

	public CountingThreadFactory(String prefix) {
		this.group = defaultGroup();
		this.namePrefix = namePrefix(prefix);
	}

	private static String namePrefix(String prefix) {
		if (prefix == null || prefix.trim().length() == 0)
			prefix = "pool";
		return prefix + "-" + poolNumber.getAndIncrement() + "-thread-";
	}

	private static ThreadGroup defaultGroup() {
		return Thread.currentThread().getThreadGroup();
	}

	@Configurable
	public void setExtendedThreadFactory(ExtendedThreadFactory factory) {
		this.factory = factory;
	}

	@Override
	public Thread newThread(Runnable r) {
		Thread t = factory.newThread(group, r, namePrefix + threadNumber.getAndIncrement());

		if (t.getPriority() != Thread.NORM_PRIORITY)
			t.setPriority(Thread.NORM_PRIORITY);

		return t;
	}
}
