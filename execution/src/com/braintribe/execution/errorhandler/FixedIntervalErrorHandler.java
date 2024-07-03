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
package com.braintribe.execution.errorhandler;

import com.braintribe.cfg.Configurable;
import com.braintribe.logging.Logger;

public abstract class FixedIntervalErrorHandler<T> implements ErrorHandler<T> {

	protected static Logger logger = Logger.getLogger(FixedIntervalErrorHandler.class);

	protected long timeout = 5000L;

	@Override
	public void reset(T context) {
		//Nothing to do here
	}

	@Override
	public void handleError(Throwable t, T context) {
		String contextInformation = this.getContextInformation(context);
		try {
			logger.debug(contextInformation+": Because of an error, we wait "+this.timeout+" ms before retrying.");
			synchronized (this) {
				wait(this.timeout);
			}
		} catch(Exception e) {
			logger.error(contextInformation+": Error while waiting for the next retry.", e);
		}
	}

	@Override
	public boolean shouldExecute(T context) {
		return true;
	}

	protected abstract String getContextInformation(T context);
	
	public long getTimeout() {
		return timeout;
	}
	@Configurable
	public void setTimeout(long timeout) {
		this.timeout = timeout;
	}

}
