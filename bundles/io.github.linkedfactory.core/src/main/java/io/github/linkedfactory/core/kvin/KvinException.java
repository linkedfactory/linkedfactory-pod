/*
 * Copyright (c) 2026 Fraunhofer IWU.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.linkedfactory.core.kvin;

/**
 * Signals that a {@link Kvin} store could not complete an operation, for
 * example because the underlying storage or a remote endpoint failed.
 * <p>
 * The exception is unchecked because results are read lazily through
 * iterators, whose methods cannot throw checked exceptions.
 */
public class KvinException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	public KvinException(String message) {
		super(message);
	}

	public KvinException(String message, Throwable cause) {
		super(message, cause);
	}

	public KvinException(Throwable cause) {
		super(cause);
	}
}
