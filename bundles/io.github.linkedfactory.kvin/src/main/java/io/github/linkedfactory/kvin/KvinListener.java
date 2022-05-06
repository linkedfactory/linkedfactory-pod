/*
 * Copyright (c) 2022 Fraunhofer IWU.
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
package io.github.linkedfactory.kvin;

import net.enilink.komma.core.URI;

/**
 * A listener to be notified of changes to the key-value store.
 */
public interface KvinListener {
	/**
	 * Called when a new (item, property) combination has been created.
	 */
	void entityCreated(URI item, URI property);

	/**
	 * Called when a new value for the (item, property, context) combination has been added.
	 */
	void valueAdded(URI item, URI property, URI context, long time, long sequenceNr, Object value);
}