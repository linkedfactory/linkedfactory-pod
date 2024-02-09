package io.github.linkedfactory.service.config;

import io.github.linkedfactory.core.kvin.Kvin;

/**
 * Factory for creating {@link Kvin} instances.
 */
public interface IKvinFactory {
	/**
	 * Creates a {@link Kvin} instance
	 * @return an instance of a {@link Kvin} store
	 */
	Kvin create();
}
