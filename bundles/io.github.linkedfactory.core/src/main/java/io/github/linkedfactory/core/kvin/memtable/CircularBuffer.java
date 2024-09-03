package io.github.linkedfactory.core.kvin.memtable;

/**
 * A fast non-blocking circular buffer backed by an array.
 *
 * @param <T> Type of elements within this buffer
 */
final class CircularBuffer<T> {

	private final T[] elements;
	private volatile int head = 0;
	private volatile int tail = 0;

	CircularBuffer(int size) {
		this.elements = (T[]) new Object[size];
	}

	boolean add(T element) {
		// faster version of:
		// tail == Math.floorMod(head - 1, elements.length)
		if (head > 0 ? tail == head - 1 : tail == elements.length - 1) {
			return false;
		}
		elements[tail] = element;
		tail = (tail + 1) % elements.length;
		return true;
	}

	T remove() {
		T result = null;
		if (tail != head) {
			result = elements[head];
			head = (head + 1) % elements.length;
		}
		return result;
	}
}