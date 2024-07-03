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
package com.braintribe.execution.virtual;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class CountingVirtualThreadFactory implements ThreadFactory {

	private static final AtomicInteger poolNumber = new AtomicInteger(1);

	private final String namePrefix;
	private final AtomicInteger threadNumber = new AtomicInteger(1);

	public CountingVirtualThreadFactory(String prefix) {
		this.namePrefix = namePrefix(prefix);
	}

	private static String namePrefix(String prefix) {
		if (prefix == null || prefix.trim().length() == 0)
			prefix = "pool";
		return prefix + "-" + poolNumber.getAndIncrement() + "-thread-";
	}

	@Override
	public Thread newThread(Runnable r) {
		Thread thread = Thread.ofVirtual().name(namePrefix + threadNumber.getAndIncrement()).unstarted(r);
		return thread;
	}
}
