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
package com.braintribe.execution.context;

import java.util.concurrent.Callable;

import com.braintribe.common.attribute.AttributeContext;
import com.braintribe.utils.collection.impl.AttributeContexts;

public class AttributeContextTransferCallable<T> implements Callable<T> {
	private AttributeContext attributeContext = AttributeContexts.peek();
	private Callable<T> callable;

	public AttributeContextTransferCallable(Callable<T> callable) {
		super();
		this.callable = callable;
	}

	@Override
	public T call() throws Exception {
		if (attributeContext != AttributeContexts.empty()) {
			return AttributeContexts.with(attributeContext).call(callable);
		} else {
			return callable.call();
		}
	}
}