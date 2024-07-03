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
package com.braintribe.execution.queue;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * Special implementation of the LinkedBlockinguQueue
 * 
 * Its intended use is for ThreadPoolExecutors only where corePoolSize == maxPoolSize.
 * This can be used to block a submit to a thread pool until one of the working threads is available again.
 */
public class LimitedQueue<T> extends LinkedBlockingQueue<T> {
	
	private static final long serialVersionUID = 1L;

	public LimitedQueue(int maxSize) {
		super(maxSize);
	}

	@Override
	public boolean offer(T e) {
		// turn offer() and add() into a blocking calls (unless interrupted)
		try {
			put(e);
			return true;
		} catch(InterruptedException ie) {
			Thread.currentThread().interrupt();
		}
		return false;
	}

}
