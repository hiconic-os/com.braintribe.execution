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

import java.util.concurrent.Callable;

/**
 * Extension of the Callable interface to allow to assign a meaningful name to the Callable object (e.g., for debugging
 * or analytics using the JMX console).
 * 
 * @author roman.kurmanowytsch
 */
public interface NamedCallable<V> extends Callable<V> {

	/**
	 * Should return a meaningful name that will be used for the thread's name.
	 * 
	 * @return The name of the callable.
	 */
	public String getName();

}
